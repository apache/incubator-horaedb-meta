package codeerr

import (
	"fmt"
)

type CodeErrorWrapper interface {
	Wrap(cause error) CodeError
}

type codeErrorWrapperImpl struct {
	code Code
	msg  string
}

func (w *codeErrorWrapperImpl) Wrap(cause error) CodeError {
	return &WrappedCodeError{
		code:  w.code,
		msg:   w.msg,
		cause: cause,
	}
}

type WrappedCodeError struct {
	code  Code
	msg   string
	cause error
}

func (e *WrappedCodeError) Error() string {
	return fmt.Sprintf("code:%d, msg:%v, cause:%v", e.code, e.msg, e.cause)
}

func (e *WrappedCodeError) Code() Code {
	return e.code
}
