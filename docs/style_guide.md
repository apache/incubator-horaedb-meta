# Style Guide
CeresMeta is written in Golang so the basic code style we adhere to is the [CodeReviewComments](https://github.com/golang/go/wiki/CodeReviewComments).

Besides the [CodeReviewComments](https://github.com/golang/go/wiki/CodeReviewComments), there are also some custom rules for the project:
- Error Handling
- Logging

## Error Handling
- Construct: define leaf errors on package level with by package [CodeError]().
- Wrap: wrap errors by `errors.Wrap/errors.Wrapf`.
- Check: test error identity by `errors.Is` (just compare the error to the ones defined in error-thrown package).
- Log: only log the error on the top level package.
- Respond: respond the [CodeError]() unwrapped by `errors.Cause` to client on service level.

## Logging
(TODO)
