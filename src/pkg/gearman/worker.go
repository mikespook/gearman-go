package gearman

import(
    "os"
    "sync"
    "log"
)

type JobFunction func(job *Job) []byte

type Worker struct {
    servers []*JobClient
    functions map[string]JobFunction

    running bool
    incoming chan *Job
    mutex sync.Mutex
    queue chan *Job
}

func NewWorker() (worker *Worker) {
    worker = &Worker{servers:make([]*JobClient, 0, WORKER_SERVER_CAP),
        functions: make(map[string]JobFunction),
        incoming: make(chan *Job, 512),
        queue: make(chan *Job, 512),
        running: true,}
    return worker
}

// add server
// worker.AddServer("127.0.0.1:4730")
func (worker * Worker) AddServer(addr string) (err os.Error) {
    worker.mutex.Lock()
    defer worker.mutex.Unlock()

    if len(worker.servers) == cap(worker.servers) {
        return os.NewError("There were too many servers.")
    }

    // Create a new job server's client as a agent of server
    server, err := NewJobClient(addr, worker.incoming)
    if err != nil {
        return err
    }

    n := len(worker.servers)
    worker.servers = worker.servers[0: n + 1]
    worker.servers[n] = server
    return
}


// add function
func (worker * Worker) AddFunction(funcname string,
    f JobFunction) (err os.Error) {
    worker.mutex.Lock()
    defer worker.mutex.Unlock()

    if f == nil {
        return os.NewError("Job function should not be nil.")
    }
    worker.functions[funcname] = f
    return
}
// work
func (worker * Worker) Work() {
    for k, _ := range worker.functions {
        job := NewJob(nil, REQ, CAN_DO, []byte(k))
        worker.Write(job)
    }

    for _, v := range worker.servers {
        go v.Work()
    }
    for worker.running {
        select {
            case job := <-worker.incoming:
                if job == nil {
                    break
                }
                switch job.DataType {
                    case ERROR:
                        log.Panicln(string(job.Data))
                    default:
                        if err := worker.Exec(job); err != nil {
                            log.Panicln(err)
                        }
                }
                worker.queue <- job
        }
    }
}

func (worker * Worker) Result() (job *Job) {
    if l := len(worker.queue); l != 1 {
        if l == 0 {
            return
        }
        for i := 0; i < l - 1; i ++ {
            <-worker.queue
        }
    }
    return <-worker.queue
}

// Close
// should used as defer
func (worker * Worker) Close() (err os.Error){
    worker.running = false
    for _, v := range worker.servers {
        err = v.Close()
    }
    close(worker.incoming)
    return err
}

func (worker * Worker) Write(job *Job) (err os.Error) {
    e := make(chan os.Error)
    for _, v := range worker.servers {
        go func() {
            job.client = v
            e <- v.WriteJob(job)
        }()
    }
    return <- e
}

// Echo
func (worker * Worker) Echo(data []byte) (err os.Error) {
    job := NewJob(nil, REQ, ECHO_REQ, data)
    return worker.Write(job)
}

// Exec
func (worker * Worker) Exec(job *Job) (err os.Error) {
    return
}
