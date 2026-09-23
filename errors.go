package loom

import "errors"

var (
	ErrInvalidSize = errors.New("loom: invalid size")
	ErrNilPool     = errors.New("loom: nil pool")
	ErrNilFunc     = errors.New("loom: nil func")
	ErrPanic       = errors.New("loom: task panic")
)
