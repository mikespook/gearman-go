package gearman

import(
    "os"
    "sync"
//    "log"
)

type JobFunction func(job *WorkerJob) ([]byte, os.Error)
type JobFunctionMap map[string]JobFunction

type Worker struct {
    clients []*jobClient
    functions JobFunctionMap

    running bool
    incoming chan *WorkerJob
    mutex sync.Mutex
    JobQueue chan *WorkerJob
    ErrQueue chan os.Error
}

func NewWorker() (worker *Worker) {
    worker = &Worker{
        // job server list
        clients:make([]*jobClient, 0, WORKER_SERVER_CAP),
        // function list
        functions: make(JobFunctionMap),
        incoming: make(chan *WorkerJob, QUEUE_CAP),
        JobQueue: make(chan *WorkerJob, QUEUE_CAP),
        ErrQueue: make(chan os.Error, QUEUE_CAP),
        running: true,
    }
    return
}

// add server
// worker.AddServer("127.0.0.1:4730")
func (worker * Worker) AddServer(addr string) (err os.Error) {
    worker.mutex.Lock()
    defer worker.mutex.Unlock()

    if len(worker.clients) == cap(worker.clients) {
        return os.NewError("There were too many clients.")
    }

    // Create a new job server's client as a agent of server
    server, err := newJobClient(addr, worker)
    if err != nil {
        return err
    }

    n := len(worker.clients)
    worker.clients = worker.clients[0: n + 1]
    worker.clients[n] = server
    return
}


// add function
func (worker * Worker) AddFunction(funcname string,
    f JobFunction, timeout uint32) (err os.Error) {
    if f == nil {
        return os.NewError("Job function should not be nil.")
    }
    if len(worker.clients) < 1 {
        return os.NewError("Did not connect to Job Server.")
    }
    worker.mutex.Lock()
    defer worker.mutex.Unlock()
    worker.functions[funcname] = f

    var datatype uint32
    var data []byte
    if timeout == 0 {
        datatype = CAN_DO
        data = []byte(funcname)
    } else {
        datatype = CAN_DO_TIMEOUT
        data = []byte(funcname + "\x00")
        t := uint32ToByte(timeout)
        data = append(data, t[:] ...)
    }
    job := NewWorkerJob(REQ, datatype, data)
    worker.WriteJob(job)
    return
}

// remove function
func (worker * Worker) RemoveFunction(funcname string) (err os.Error) {
    worker.mutex.Lock()
    defer worker.mutex.Unlock()

    if worker.functions[funcname] == nil {
        return os.NewError("No function named: " + funcname)
    }
    worker.functions[funcname] = nil, false
    job := NewWorkerJob(REQ, CANT_DO, []byte(funcname))
    worker.WriteJob(job)
    return
}

// work
func (worker * Worker) Work() {
    for _, v := range worker.clients {
        go v.Work()
    }
    for worker.running {
        select {
            case job := <-worker.incoming:
                if job == nil {
                    break
                }
                switch job.dataType {
                    case NO_JOB:
                        // do nothing
                    case ERROR:
                        _, err := getError(job.Data)
                        worker.ErrQueue <- err
                    case JOB_ASSIGN, JOB_ASSIGN_UNIQ:
                        if err := worker.exec(job); err != nil {
                            worker.ErrQueue <- err
                        }
                        fallthrough
                    default:
                        worker.JobQueue <- job
                }
        }
    }
}

func (worker * Worker) LastResult() (job *WorkerJob) {
    if l := len(worker.JobQueue); l != 1 {
        if l == 0 {
            return
        }
        for i := 0; i < l - 1; i ++ {
            <-worker.JobQueue
        }
    }
    return <-worker.JobQueue
}

// Close
// should used as defer
func (worker * Worker) Close() (err os.Error){
    worker.running = false
    for _, v := range worker.clients {
        err = v.Close()
    }
    close(worker.incoming)
    return err
}

func (worker * Worker) WriteJob(job *WorkerJob) (err os.Error) {
    e := make(chan os.Error)
    for _, v := range worker.clients {
        go func() {
            e <- v.WriteJob(job)
        }()
    }
    return <- e
}

// Echo
func (worker * Worker) Echo(data []byte) (err os.Error) {
    job := NewWorkerJob(REQ, ECHO_REQ, data)
    return worker.WriteJob(job)
}

// Reset
func (worker * Worker) Reset() (err os.Error){
    job := NewWorkerJob(REQ, RESET_ABILITIES, nil)
    return worker.WriteJob(job)
}

// SetId
func (worker * Worker) SetId(id string) (err os.Error) {
    job := NewWorkerJob(REQ, SET_CLIENT_ID, []byte(id))
    return worker.WriteJob(job)
}

// Exec
func (worker * Worker) exec(job *WorkerJob) (err os.Error) {
    jobdata := splitByteArray(job.Data, '\x00')
    job.Handle = string(jobdata[0])
    funcname := string(jobdata[1])
    if job.dataType == JOB_ASSIGN {
        job.Data = jobdata[2]
    } else {
        job.UniqueId = string(jobdata[2])
        job.Data = jobdata[3]
    }
    f := worker.functions[funcname]
    if f == nil {
        return os.NewError("function is nil")
    }
    result, err := f(job)
    var datatype uint32
    if err == nil {
        datatype = WORK_COMPLETE
    } else{
        if result == nil {
            datatype = WORK_FAIL
        } else {
            datatype = WORK_EXCEPTION
        }
    }

    job.magicCode = REQ
    job.dataType = datatype
    job.Data = result

    worker.WriteJob(job)
    return
}
