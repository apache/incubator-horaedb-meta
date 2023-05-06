// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

package scheduler

import "github.com/pkg/errors"

type TopologyType string

const (
	TopologyTypeStatic  = "static"
	TopologyTypeDynamic = "dynamic"
)

func ParseTopologyType(rawString string) (TopologyType, error) {
	switch rawString {
	case TopologyTypeStatic:
		return TopologyTypeStatic, nil
	case TopologyTypeDynamic:
		return TopologyTypeDynamic, nil
	}

	return "", errors.WithMessagef(ErrParseTopologyType, "rawString:%s could not be parsed to topologyType", rawString)
}
