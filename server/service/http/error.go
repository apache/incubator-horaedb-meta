package http

import "github.com/CeresDB/ceresmeta/pkg/coderr"

var (
	ErrParseRequest = coderr.NewCodeError(coderr.BadRequest, "parse request params failed")
)
