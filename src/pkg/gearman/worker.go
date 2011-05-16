package gearman

import(
    "net"
    "os"
)

type Worker struct {

    servers []net.Conn
}

func NewWorker() (worker *Worker) {
    worker = &Worker{servers:make([]net.Conn, 0, WORKER_SERVER_CAP)}
    return worker
}


// add server
// worker.AddServer("127.0.0.1:4730")
func (worker * Worker) AddServer(addr string) (err os.Error) {
    if len(worker.servers) == cap(worker.servers) {
        return os.NewError("There were too many servers.")
    }
    conn, err := net.Dial(TCP, addr)
    if err != nil {
        return err
    }
    n := len(worker.servers)
    worker.servers = worker.servers[0: n + 1]
    worker.servers[n] = conn
    return nil
}
/*

// add function
func (worker * Worker) AddFunction(funcname string, 
    f interface{}, context interface{}) (err Error) {

}

// work
func (worker * GearmanWorker) Work() {
    for {
        
    }
}

// Close
// should used as defer
func (worker * GearmanWorker) Close() (err Error){
    
}
*/
