package codeerr

import "net/http"

type Code int

const (
	InvalidParams Code = http.StatusBadRequest
	Internal           = http.StatusInternalServerError
)
const HTTPCodeUpperBound int = 1000

func (c Code) ToHTTPCode() int {
	if i := int(c); i < HTTPCodeUpperBound {
		return i
	}

	// TODO: use switch to convert the code to http code.
	return int(c)
}
