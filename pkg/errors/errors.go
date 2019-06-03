package errors

import (
	"fmt"

	"github.com/pkg/errors"
)

var (
	New    = errors.New
	Errorf = errors.Errorf
	Wrap   = errors.Wrap
	Wrapf  = errors.Wrapf
)

// github.com/pkg/errors can be formatted with rich information, including stacktrace, see:
// 	https://godoc.org/github.com/pkg/errors#hdr-Formatted_printing_of_errors
type richError interface {
	error
	fmt.Formatter
}

// wrap as necessary an object with rich (stacktrace esp.) information.
func RichError(err interface{}) error {
	if err == nil {
		return nil
	}
	switch err := err.(type) {
	case richError:
		return err
	case error:
		return errors.Wrap(err, err.Error()).(richError)
	default:
		return errors.New(fmt.Sprintf("%s", err)).(richError)
	}
}
