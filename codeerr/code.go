package codeerr

import "net/http"

type Code int

const (
	InvalidParams Code = http.StatusBadRequest
	Internal           = http.StatusInternalServerError
)
const HttpCodeUpperBound int = 1000

func (c Code) ToHttpCode() int {
	if int(c) < HttpCodeUpperBound {
		return c
	}

	switch c {
	default:
		return http.StatusInternalServerError
	}
}
