// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package http

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure"
	"go.uber.org/zap"
)

type API struct {
	procedureManager procedure.Manager
	procedureFactory *procedure.Factory

	clusterManager cluster.Manager
}

func NewAPI(procedureManager procedure.Manager, procedureFactory *procedure.Factory, clusterManager cluster.Manager) *API {
	return &API{
		procedureManager: procedureManager,
		procedureFactory: procedureFactory,
		clusterManager:   clusterManager,
	}
}

func (a *API) NewAPIRouter() *Router {
	router := New().WithPrefix("/api/v1").WithInstrumentation(printRequestInsmt)

	router.Post("/transferLeader", a.transferLeader)

	return router
}

// printRequestInsmt used for printing every request information.
func printRequestInsmt(handlerName string, handler http.HandlerFunc) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			log.Error("parse http body failed", zap.String("handlerName", handlerName))
			body = []byte("")
		}
		log.Info("receive http request", zap.String("handlerName", handlerName), zap.String("client host", request.RemoteAddr), zap.String("method", request.Method), zap.String("params", request.Form.Encode()), zap.String("body", string(body)))
		handler.ServeHTTP(writer, request)
	}
}

type TransferLeaderRequest struct {
	ShardID           uint64 `json:"shardID"`
	NewLeaderNodeName string `json:"newLeaderNodeName"`
}

// TODO: impl this function
func (a *API) transferLeader(_ http.ResponseWriter, req *http.Request) {
	var transferLeaderRequest TransferLeaderRequest
	err := json.NewDecoder(req.Body).Decode(&transferLeaderRequest)
	if err != nil {
		log.Error("decode request body failed")
		return
	}
}
