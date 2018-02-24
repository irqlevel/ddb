package mds

import (
	"fmt"
)

var (
	ErrNotImplemented = fmt.Errorf("Not implemented")
	ErrJsonEncode     = fmt.Errorf("Json encode failure")
	ErrJsonDecode     = fmt.Errorf("Json decode failure")
	ErrNotFound       = fmt.Errorf("Not found")
	ErrAlreadyExists  = fmt.Errorf("Already exists")
	ErrBadRequest     = fmt.Errorf("Bad request")
)
