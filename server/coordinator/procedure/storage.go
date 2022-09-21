package procedure

import (
	"context"
)

type Write interface {
	CreateOrUpdate(ctx context.Context, meta *Meta) error
}

// nolint
type Meta struct {
	id      uint64
	typ     uint8
	rawData []byte
}

type Storage interface {
	Write
	List(ctx context.Context, state State) (*Meta, error)
}
