package coderr

// CodeError is an error with an extra method Code().
type CodeError interface {
	error
	Code() Code
}
