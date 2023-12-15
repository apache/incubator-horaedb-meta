/*
 * Copyright 2022 The CeresDB Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package eventdispatch

import (
	"context"
	"github.com/CeresDB/ceresdbproto/golang/pkg/metaeventpb"

	"github.com/CeresDB/ceresmeta/server/cluster/metadata"
)

type Dispatch interface {
	OpenShard(context context.Context, address string, request OpenShardRequest) error
	CloseShard(context context.Context, address string, request CloseShardRequest) error
	CreateTableOnShard(context context.Context, address string, request CreateTableOnShardRequest) (uint64, error)
	DropTableOnShard(context context.Context, address string, request DropTableOnShardRequest) (uint64, error)
	OpenTableOnShard(ctx context.Context, address string, request OpenTableOnShardRequest) (OpenTableOnShardResponse, error)
	CloseTableOnShard(context context.Context, address string, request CloseTableOnShardRequest) (CloseTableOnShardResponse, error)
}

type OpenShardRequest struct {
	Shard metadata.ShardInfo
}

type CloseShardRequest struct {
	ShardID uint32
}

type UpdateShardInfo struct {
	CurrShardInfo metadata.ShardInfo
}

type CreateTableOnShardRequest struct {
	UpdateShardInfo  UpdateShardInfo
	TableInfo        metadata.TableInfo
	EncodedSchema    []byte
	Engine           string
	CreateIfNotExist bool
	Options          map[string]string
}

type DropTableOnShardRequest struct {
	UpdateShardInfo UpdateShardInfo
	TableInfo       metadata.TableInfo
}

type OpenTableOnShardRequest struct {
	UpdateShardInfo UpdateShardInfo
	TableInfo       metadata.TableInfo
}

type OpenTableOnShardResponse struct {
	Version uint64
}

type CloseTableOnShardRequest struct {
	UpdateShardInfo UpdateShardInfo
	TableInfo       metadata.TableInfo
}

type CloseTableOnShardResponse struct {
	Version uint64
}

func convertOpenTableOnShardResponse(resp *metaeventpb.OpenTableOnShardResponse) OpenTableOnShardResponse {
	return OpenTableOnShardResponse{
		Version: 1,
	}
}

func convertCloseTableOnShardResponse(resp *metaeventpb.CloseTableOnShardResponse) CloseTableOnShardResponse {
	return CloseTableOnShardResponse{
		Version: 1,
	}
}
