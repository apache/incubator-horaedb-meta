# Style Guide
CeresMeta is written in Golang so the basic code style we adhere to is the [CodeReviewComments](https://github.com/golang/go/wiki/CodeReviewComments).

Besides the [CodeReviewComments](https://github.com/golang/go/wiki/CodeReviewComments), there are also some custom rules for the project:
- Error Handling
- Logging

## Error Handling
### Principles
- Construct: define leaf errors on package level(often in a separate `error.go` file) by package [coderr](https://github.com/CeresDB/ceresmeta/tree/main/coderr).
- Wrap: wrap errors by `errors.Wrap/errors.Wrapf`.
- Check: test error identity by `errors.Is` (just compare the error to the ones defined in error-thrown package).
- Log: only log the error on the top level package.
- Respond: respond the `CodeError`(defined in package [coderr](https://github.com/CeresDB/ceresmeta/tree/main/coderr)) unwrapped by `errors.Cause` to client on service level.

### Example
```go
// filename: server/error.go
package server
import "github.com/CeresDB/ceresmeta/coderr"

var (
    ErrCreateEtcdClient = coderr.NewCodeErrorWrapper(coderr.Internal, "fail to create etcd client")
    ErrStartEtcd        = coderr.NewCodeErrorWrapper(coderr.Internal, "fail to start embed etcd")
)

var ErrStartEtcdTimeout = coderr.NewNormalizedCodeError(coderr.Internal, "fail to start etcd server in time")
```

```go
// filename: server/server.go
func (srv *Server) startEtcd() error {
    etcdSrv, err := embed.StartEtcd(srv.etcdCfg)
    if err != nil {
        return ErrStartEtcd.Wrap(err)
    }
	return nil
}
```

```go
func main() {
    err := srv.startEtcd()
	cerr := err.Cause()
	if errors.Is(cerr, ErrStartEtcd) {
		code := cerr.Code()
		log.Fatalf("fail to start etcd server, code:%d, err:%v", code, cerr)
    }
	
	return
}
```

## Logging
(TODO)
