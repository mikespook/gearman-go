package gearman

// #cgo LDFLAGS: -lgearman
// #include <libgearman/gearman.h>
import "C"

import(
    "log"
    "unsafe"
)

type GearmanWorker struct {
    worker C.gearman_worker_st
}
// Create gearman worker
func GearmanWorkerCreate() * GearmanWorker {

    worker := new(GearmanWorker)
    if C.gearman_worker_create(&worker.worker) == nil {
        log.Panic("Memory allocation failure on worker creation")
    }
    return worker
}

// get error
func (worker * GearmanWorker) Error() string {
    return C.GoString(C.gearman_worker_error(&worker.worker))
}

// add server
func (worker * GearmanWorker) AddServer(host string, port uint16) {
    h := C.CString(host)
    defer C.free(unsafe.Pointer(h))
    if C.gearman_worker_add_server(&worker.worker, h,
        C.in_port_t(port)) != C.GEARMAN_SUCCESS {
        log.Panic(worker.Error())
    }
}

// add function
func (worker * GearmanWorker) AddFunction(funcname string, timeout uint32, f interface{}, context interface{}) {
    fn := C.CString(funcname)
    defer C.free(unsafe.Pointer(fn))
    if C.gearman_worker_add_function(&worker.worker, fn, C.uint32_t(timeout), C.gearman_worker_fn(&unsafe.Pointer(&f)), unsafe.Pointer(&context)) != C.GEARMAN_SUCCESS {
        log.Panic(worker.Error())
    }
}

// work
func (worker * GearmanWorker) Work() {
    for {
        if C.gearman_worker_work(&worker.worker) != C.GEARMAN_SUCCESS {
            log.Panic(worker.Error())
        }
    }
}

// free
// should used as defer
func (worker * GearmanWorker) Free() {
    C.gearman_worker_free(&worker.worker)
}
