package service

import (
	"errors"
	"testing"

	"github.com/itering/subscan-plugin/storage"
	"github.com/itering/subscan/plugins/system/model"
	"github.com/stretchr/testify/require"
)

type projectionStorage struct {
	storage.Dao
	err error
	row *model.ExtrinsicError
}

func (d *projectionStorage) Create(row interface{}) error {
	if d.err != nil {
		return d.err
	}
	d.row = row.(*model.ExtrinsicError)
	return nil
}

func TestDispatchErrorProjectionPropagatesFailure(t *testing.T) {
	d := &projectionStorage{err: errors.New("injected persistence failure")}
	s := New(d)
	e := &storage.Event{BlockNum: 42, ExtrinsicIdx: 1}
	params := []storage.EventParam{{Type: "DispatchError", Value: map[string]interface{}{"BadOrigin": nil}}}
	require.ErrorIs(t, s.ExtrinsicFailed(1, e, params), d.err)
	require.Nil(t, d.row)
	d.err = nil
	require.NoError(t, s.ExtrinsicFailed(1, e, params))
	require.Equal(t, "42-1", d.row.ExtrinsicIndex)
	require.Equal(t, "BadOrigin", d.row.Name)
	require.Error(t, s.ExtrinsicFailed(1, e, nil), "missing error details must not acknowledge a projection")
	require.Error(t, s.ExtrinsicFailed(1, e, []storage.EventParam{{Type: "DispatchError", Value: map[string]interface{}{"unrecognized": nil}}}))
}
