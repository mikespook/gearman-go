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
type objectId struct {
    bson.ObjectId
}

func (id *objectId) Id() string {
    return id.String()
}

func NewObjectId() IdGenerator {
    return &objectId{bson.NewObjectId()}
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
