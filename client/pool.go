// Copyright 2011 Xing Xing <mikespook@gmail.com>.
// All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package client

import (
    "sync"
    "time"
    "errors"
    "math/rand"
    "github.com/mikespook/gearman-go/common"
)

const (
    PoolSize = 10
)

var (
    ErrNotFound = errors.New("Server Not Found")
)

type poolClient struct {
    *Client
    Rate int
}

type SelectionHandler func(map[string]*poolClient, string) string

func SelectWithRate(pool map[string]*poolClient,
last string) (addr string) {
    total := 0
    for _, item := range pool {
        total += item.Rate
        if rand.Intn(total) < item.Rate {
            return item.addr
        }
    }
    return last
}

func SelectRandom(pool map[string]*poolClient,
last string) (addr string) {
    r := rand.Intn(len(pool))
    i := 0
    for k, _ := range pool {
        if r == i {
            return k
        }
        i ++
    }
    return last
}



type Pool struct {
    SelectionHandler SelectionHandler
    ErrHandler common.ErrorHandler

    clients map[string]*poolClient
    last string

    mutex sync.Mutex
}

// Create a new pool.
func NewPool() (pool *Pool) {
    return &Pool{
        clients: make(map[string]*poolClient, PoolSize),
        SelectionHandler: SelectWithRate,
    }
}

// Add a server with rate.
func (pool *Pool) Add(addr string, rate int) (err error) {
    pool.mutex.Lock()
    defer pool.mutex.Unlock()
    var item *poolClient
    var ok bool
    if item, ok = pool.clients[addr]; ok {
        item.Rate = rate
    } else {
        var client *Client
        client, err = New(addr)
        item = &poolClient{Client: client, Rate: rate}
        pool.clients[addr] = item
    }
    return
}

// Remove a server.
func (pool *Pool) Remove(addr string) {
    pool.mutex.Lock()
    defer pool.mutex.Unlock()
    delete(pool.clients, addr)
}

func (pool *Pool) Do(funcname string, data []byte,
flag byte, h JobHandler) (addr, handle string) {
    client := pool.selectServer()
    handle = client.Do(funcname, data, flag, h)
    addr = client.addr
    return
}

func (pool *Pool) DoBg(funcname string, data []byte,
flag byte) (addr, handle string) {
    client := pool.selectServer()
    handle = client.DoBg(funcname, data, flag)
    addr = client.addr
    return
}

// Get job status from job server.
// !!!Not fully tested.!!!
func (pool *Pool) Status(addr, handle string, timeout time.Duration) (status *Status, err error) {
    if client, ok := pool.clients[addr]; ok {
        status, err = client.Status(handle, timeout)
    } else {
        err = ErrNotFound
    }
    return
}

// Send a something out, get the samething back.
func (pool *Pool) Echo(addr string, data []byte, timeout time.Duration) (r []byte, err error) {
    var client *poolClient
    if addr == "" {
        client = pool.selectServer()
    } else {
        var ok bool
        if client, ok = pool.clients[addr]; !ok {
            err = ErrNotFound
            return
        }
    }
    r, err = client.Echo(data, timeout)
    return
}

// Close
func (pool *Pool) Close() (err map[string]error) {
    err = make(map[string]error)
    for _, c := range pool.clients {
        err[c.addr] = c.Close()
    }
    return
}

func (pool *Pool) selectServer() (client *poolClient) {
    for client == nil {
        addr := pool.SelectionHandler(pool.clients, pool.last)
        var ok bool
        if client, ok = pool.clients[addr]; ok {
            pool.last = addr
            break
        }
    }
    return
}
