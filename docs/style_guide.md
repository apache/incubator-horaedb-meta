# Style Guide
CeresMeta is written in Golang so the basic code style we adhere to is the [CodeReviewComments](https://github.com/golang/go/wiki/CodeReviewComments).

Besides the [CodeReviewComments](https://github.com/golang/go/wiki/CodeReviewComments), there are also some custom rules for the project:
- Error Handling
- Logging

## Error Handling
### Principles
- Global error code:
  - Any error defined in the repo should be assigned an error code.
  - An error code can be used by multiple different errors.
  - The error codes are defined in the single global package [codeerr](https://github.com/CeresDB/ceresmeta/tree/main/coderr).
- Construct: define leaf errors on package level(often in a separate `error.go` file) by package [coderr](https://github.com/CeresDB/ceresmeta/tree/main/coderr).
- Wrap: wrap errors by `errors.Wrap/errors.Wrapf`.
- Check: test the error identity by calling `coderr.EqualsByCode` or `coderr.EqualsByValue` should always work.
- Log: only log the error on the top level package.
- Respond: respond the `CodeError`(defined in package [coderr](https://github.com/CeresDB/ceresmeta/tree/main/coderr)) unwrapped by `errors.Cause` to client on service level.

### Example
```go
// filename: server/error.go
package server
import "github.com/CeresDB/ceresmeta/coderr"

var (
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
	if err != nil {
		return 
    }
	if coderr.EqualsByCode(err, coderr.Internal) {
        log.Errorf("internal error, err:%v", err)
    }
    if coderr.EqualsByValue(err, server.EtcdStartEtcdTimeout) {
		log.Errorf("start etcd server timeout, err:%v", err)
    }
	
	cerr, ok := err.(coderr.CodeError)
	if ok {
	    log.Errorf("error code is:%v", cerr.Code())	
    } else {
	    log.Errorf("not a CodeError, err:%v", err)	
    }
		
	return
}
```

## Logging
(TODO)
