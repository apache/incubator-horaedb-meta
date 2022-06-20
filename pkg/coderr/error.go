// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package coderr

import "github.com/pkg/errors"

// CodeError is an error with an extra method Code().
type CodeError interface {
	error
	Code() Code
}

// EqualsByCode checks whether the cause of `err` is the kind of error specified by the expectCode.
// Returns false if the cause of `err` is not CodeError.
func EqualsByCode(err error, expectCode Code) bool {
	cause := errors.Cause(err)
	cerr, ok := cause.(CodeError)
	if !ok {
		return false
	}
	return expectCode == cerr.Code()
}

// EqualsByValue checks whether the cause of `err` is the expectErr.
func EqualsByValue(err error, expectErr error) bool {
	cause := errors.Cause(err)
	return errors.Is(cause, expectErr)
}
