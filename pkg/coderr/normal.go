// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package coderr

import "fmt"

var _ CodeError = &NormalCodeError{}

// NormalCodeError is actually the leaf error in the error chain, that is to say, the error is generated in our codebase.
type NormalCodeError struct {
	code Code
	msg  string
}

func (e *NormalCodeError) Error() string {
	return fmt.Sprintf("code:%d, msg:%s", e.code, e.msg)
}

func (e *NormalCodeError) Code() Code {
	return e.code
}

func NewNormalizedCodeError(code Code, msg string) CodeError {
	return &NormalCodeError{
		code,
		msg,
	}
}
