package syncdb

import (
	"github.com/rumlang/rum/parser"
	"github.com/rumlang/rum/runtime"
)

func rumParse(s string) (*parser.Value, error) {
	v, err := parser.Parse(parser.NewSource(s))
	if err != nil {
		return nil, err
	}
	return &v, nil
}

func rumEval(s string, c *runtime.Context) (*parser.Value, error) {
	ival, err := rumParse(s)
	if err != nil {
		return nil, err
	}

	val, err := c.TryEval(*ival)
	if err != nil {
		return nil, err
	}
	return &val, nil
}
