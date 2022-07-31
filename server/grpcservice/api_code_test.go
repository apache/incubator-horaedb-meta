// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package grpcservice

import (
	"net/http"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestConvertAPICode(t *testing.T) {
	assert.Equal(t, ConvertRPCErrorToAPICode(nil, "cluster not found......."), APICodeClusterNotFound)
	assert.Equal(t, ConvertRPCErrorToAPICode(nil, "schema not found......."), APICodeSchemaNotFound)

	assert.Equal(t, ConvertRPCErrorToAPICode(nil, "connection refused"), http.StatusServiceUnavailable)
	assert.Equal(t, ConvertRPCErrorToAPICode(errors.New("connection refused"), ""), http.StatusServiceUnavailable)
	assert.Equal(t, ConvertRPCErrorToAPICode(errors.New("connection resdffused"), ""), http.StatusInternalServerError)
}
