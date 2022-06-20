package codeerr

import "fmt"

type NormalCodeError struct {
	code Code
	msg  string
}

func (e *NormalCodeError) Error() string {
	return fmt.Sprintf("code:%d, msg:%v", e.code, e.msg)
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

func NewCodeErrorWrapper(code Code, msg string) CodeErrorWrapper {
	return &codeErrorWrapperImpl{
		code,
		msg,
	}
}
