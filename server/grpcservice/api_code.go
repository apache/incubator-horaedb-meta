package grpcservice

import (
	"net/http"
	"strings"
)

const (
	APICodeClusterNotFound = iota + 4000
	APICodeSchemaNotFound  = iota + 4000
)

var (
	logicalKeyWordsMapping = map[string]int{
		"cluster not found": APICodeClusterNotFound,
		"schema not found":  APICodeSchemaNotFound,
	}
)

// ConvertRPCErrorToAPICode converts rpc error and its message to api code(APICode* or http codes).
func ConvertRPCErrorToAPICode(err error, errMsg string) int {
	if err == nil && len(errMsg) == 0 {
		return http.StatusOK
	}

	if err != nil {
		errMsg += err.Error()
	}

	for k, v := range logicalKeyWordsMapping {
		if strings.Contains(errMsg, k) {
			return v
		}
	}

	return convertRPCErrorToHTTPCode(errMsg)
}

func convertRPCErrorToHTTPCode(errMsg string) int {
	// TODO: gRPC Unavailable code need convert to logical code, and other gRPC code need return as HttpStatusCode

	if strings.Contains(errMsg, "connection refused") {
		return http.StatusServiceUnavailable
	}

	return http.StatusInternalServerError
}
