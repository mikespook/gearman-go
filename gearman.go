// Copyright 2011 Xing Xing <mikespook@gmail.com> All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

/*
This module is Gearman API for golang. 
The protocol was implemented by native way.
*/

package gearman

const (
    // Job type
    // JOB_NORMAL | JOB_BG means a normal level job run in background
    // normal level
    JOB_NORMAL = 0
    // background job
    JOB_BG = 1
    // low level
    JOB_LOW = 2
    // high level
    JOB_HIGH = 4
)
