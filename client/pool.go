// Copyright 2011 Xing Xing <mikespook@gmail.com>.
// All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package client

import (
    "time"
    "errors"
    "math/rand"
    "github.com/mikespook/gearman-go/common"
)

const (
    PoolSize = 10
    DefaultRetry = 5
)

var (
    ErrTooMany = errors.New("Too many errors occurred.")
)

type poolItem struct {
    *Client
    Rate int
    Addr string
}

func (item *poolItem) connect(pool *Pool) (err error) {
    item.Client, err = New(item.Addr);
    item.ErrHandler = pool.ErrHandler
    item.JobHandler = pool.JobHandler
    item.StatusHandler = pool.StatusHandler
    item.TimeOut = pool.TimeOut
    return
}


type SelectionHandler func(map[string]*poolItem, string) string

func SelectWithRate(pool map[string]*poolItem,
last string) (addr string) {
    total := 0
    for _, item := range pool {
        total += item.Rate
        if rand.Intn(total) < item.Rate {
            return item.Addr
        }
    }
    return last
}

func SelectRandom(pool map[string]*poolItem,
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
    JobHandler JobHandler
    StatusHandler StatusHandler
    TimeOut time.Duration
    Retry int

    items map[string]*poolItem
    last string
    handles map[string]string
}

// Create a new pool.
func NewPool() (pool *Pool) {
    return &Pool{
        items: make(map[string]*poolItem, PoolSize),
        Retry: DefaultRetry,
        SelectionHandler: SelectWithRate,
    }
}

// Add a server with rate.
func (pool *Pool) Add(addr string, rate int) {
    var item *poolItem
    var ok bool
    if item, ok = pool.items[addr]; ok {
        item.Rate = rate
    } else {
        item = &poolItem{Rate: rate, Addr: addr}
        item.connect(pool)
        pool.items[addr] = item
    }
}

func (pool *Pool) Do(funcname string, data []byte,
flag byte) (addr, handle string, err error) {
    for i := 0; i < pool.Retry; i ++ {
        addr = pool.SelectionHandler(pool.items, pool.last)
        item, ok := pool.items[addr]
        if ok {
            pool.last = addr
            handle, err = item.Do(funcname, data, flag)
            // error handling
            // mapping the handle to the server
            return
        }
    }
    err = ErrTooMany
    return
}

// Get job status from job server.
// !!!Not fully tested.!!!
func (pool *Pool) Status(addr, handle string) {
    if item, ok := pool.items[addr]; ok {
        item.Status(handle)
    }
}

// Send a something out, get the samething back.
func (pool *Pool) Echo(data []byte) {
    for i := 0; i < pool.Retry; i ++ {
        addr = pool.SelectionHandler(pool.items, pool.last)
        item, ok := pool.items[addr]
        if ok {
            pool.last = addr
            item.Echo(data)
        }
    }
}

// Close
func (pool *Pool) Close() (err error) {
    for _, c := range pool.items {
        err = c.Close()
    }
    return
}
