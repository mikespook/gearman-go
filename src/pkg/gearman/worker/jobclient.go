package gearman

import (
    "net"
    "os"
//    "log"
)

type jobClient struct {
    conn net.Conn
    incoming chan *Job
    running bool
}

func newJobClient(addr string, incoming chan *Job) (jobclient *jobClient, err os.Error) {
    conn, err := net.Dial(TCP, addr)
    if err != nil {
        return nil, err
    }
    jobclient = &jobClient{conn:conn, incoming: incoming, running:true}
    return jobclient, err
}

func (client *jobClient) Work() (err os.Error) {
    noop := true
    for client.running {
        // grab job
        if noop {
            client.WriteJob(NewJob(REQ, GRAB_JOB, nil))
        }
        var rel []byte
        for {
            buf := make([]byte, 2048)
            n, err := client.conn.Read(buf)
            if err != nil {
                if err == os.EOF && n == 0 {
                    break
                }
                return err
            }
            rel = append(rel, buf[0: n] ...)
            break
        }
        job, err := DecodeJob(rel)
        if err != nil {
            return err
        } else {
            switch(job.dataType) {
                case NOOP:
                    noop = true
                case NO_JOB:
                    noop = false
                    client.WriteJob(NewJob(REQ, PRE_SLEEP, nil))
                case ECHO_RES, JOB_ASSIGN_UNIQ, JOB_ASSIGN:
                    job.client = client
                    client.incoming <- job
            }
        }
    }
    return
}

func (client *jobClient) WriteJob(job * Job) (err os.Error) {
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
