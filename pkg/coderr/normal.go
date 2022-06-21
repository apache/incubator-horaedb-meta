// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package coderr

import "fmt"

var _ CodeError = &normalCodeError{}

// normalCodeError is actually the leaf error in the error chain, that is to say, the error is generated in our codebase.
type normalCodeError struct {
	code Code
	msg  string
}

func (e *normalCodeError) Error() string {
	return fmt.Sprintf("code:%d, msg:%s", e.code, e.msg)
}

func (e *normalCodeError) Code() Code {
	return e.code
}

func NewNormalizedCodeError(code Code, msg string) CodeError {
	return &normalCodeError{
		code,
		msg,
	}
}
