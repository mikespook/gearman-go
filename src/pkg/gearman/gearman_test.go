package main

import (
    "gearman"
    "fmt"
)

func main() {
    worker := gearman.GearmanWorkerCreate()
    worker.AddServer("127.0.0.1", 4730)
    worker.Work()
    worker.Free()
}
