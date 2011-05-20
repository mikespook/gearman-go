package gearman

import (
    "net"
    "os"
//    "log"
)

type jobClient struct {
    conn net.Conn
    worker *Worker
    running bool
}

func newJobClient(addr string, worker *Worker) (jobclient *jobClient, err os.Error) {
    conn, err := net.Dial(TCP, addr)
    if err != nil {
        return nil, err
    }
    jobclient = &jobClient{conn:conn, worker:worker, running:true}
    return jobclient, err
}

func (client *jobClient) Work() {
    noop := true
    OUT: for client.running {
        // grab job
        if noop {
            client.WriteJob(NewWorkerJob(REQ, GRAB_JOB, nil))
        }
        var rel []byte
        for {
            buf := make([]byte, BUFFER_SIZE)
            n, err := client.conn.Read(buf)
            if err != nil {
                if err == os.EOF && n == 0 {
                    break
                }
                client.worker.ErrQueue <- err
                continue OUT
            }
            rel = append(rel, buf[0: n] ...)
            if n < BUFFER_SIZE {
                break
            }
        }
        job, err := DecodeWorkerJob(rel)
        if err != nil {
            client.worker.ErrQueue <- err
            continue
        } else {
            switch(job.dataType) {
                case NOOP:
                    noop = true
                case NO_JOB:
                    noop = false
                    client.WriteJob(NewWorkerJob(REQ, PRE_SLEEP, nil))
                case ECHO_RES, JOB_ASSIGN_UNIQ, JOB_ASSIGN:
                    job.client = client
                    client.worker.incoming <- job
            }
        }
    }
    return
}

func (client *jobClient) WriteJob(job *WorkerJob) (err os.Error) {
    return client.Write(job.Encode())
}

func (client *jobClient) Write(buf []byte) (err os.Error) {
    var n int
    for i := 0; i < len(buf); i += n {
        n, err = client.conn.Write(buf[i:])
        if err != nil {
            return err
        }
    }
    return
}

func (client *jobClient) Close() (err os.Error) {
    client.running = false
    err = client.conn.Close()
    return
}
