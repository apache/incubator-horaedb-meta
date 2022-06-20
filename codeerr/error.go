package codeerr

type CodeError interface {
	error
	Code() Code
}
