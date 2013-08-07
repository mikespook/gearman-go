package client

import (
    "strconv"
    "labix.org/v2/mgo/bson"
    "github.com/mikespook/golib/autoinc"
)

type IdGenerator interface {
    Id() string
}

// ObjectId
type objectId struct {}

func (id *objectId) Id() string {
    return bson.NewObjectId().Hex()
}

func NewObjectId() IdGenerator {
    return &objectId{}
}

// AutoIncId
type autoincId struct {
    *autoinc.AutoInc
}

func (id *autoincId) Id() string {
    return strconv.Itoa(id.AutoInc.Id())
}

func NewAutoIncId() IdGenerator {
    return &autoincId{autoinc.New(1, 1)}
}
