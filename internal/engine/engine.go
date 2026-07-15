package engine

import (
	"fmt"
	"sync"

	"github.com/space-node/space-node/internal/types"
)

// Engine is the first in-memory vertical slice of the educational streaming DB.
type Engine struct {
	mu      sync.RWMutex
	streams map[string]types.Schema
	views   map[string]*materializedSumView
}

// New creates an empty in-memory engine.
func New() *Engine {
	return &Engine{streams: map[string]types.Schema{}, views: map[string]*materializedSumView{}}
}

// CreateStream registers a stream schema.
func (e *Engine) CreateStream(name string, schema types.Schema) error {
	if name == "" {
		return fmt.Errorf("stream name is required")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, exists := e.streams[name]; exists {
		return fmt.Errorf("stream %q already exists", name)
	}
	e.streams[name] = schema
	return nil
}

// CreateSumView registers the first supported materialized view type.
func (e *Engine) CreateSumView(spec SumViewSpec) error {
	view, err := newMaterializedSumView(spec)
	if err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	schema, ok := e.streams[spec.Source]
	if !ok {
		return fmt.Errorf("stream %q does not exist", spec.Source)
	}
	if !schema.HasColumn(spec.GroupKey) {
		return fmt.Errorf("stream %q has no group key column %q", spec.Source, spec.GroupKey)
	}
	if !schema.HasColumn(spec.SumField) {
		return fmt.Errorf("stream %q has no sum column %q", spec.Source, spec.SumField)
	}
	if _, exists := e.views[spec.Name]; exists {
		return fmt.Errorf("view %q already exists", spec.Name)
	}
	e.views[spec.Name] = view
	return nil
}

// Insert appends one row to a stream and incrementally updates dependent views.
func (e *Engine) Insert(epoch uint64, stream string, row types.Row) error {
	change := types.Change{Epoch: epoch, Op: types.Insert, Source: stream, Row: row.Clone()}
	e.mu.RLock()
	if _, ok := e.streams[stream]; !ok {
		e.mu.RUnlock()
		return fmt.Errorf("stream %q does not exist", stream)
	}
	views := make([]*materializedSumView, 0, len(e.views))
	for _, view := range e.views {
		views = append(views, view)
	}
	e.mu.RUnlock()

	for _, view := range views {
		if err := view.apply(change); err != nil {
			return err
		}
	}
	return nil
}

// GetViewRow returns one materialized-view row by key.
func (e *Engine) GetViewRow(viewName, key string) (types.Row, bool) {
	e.mu.RLock()
	view, ok := e.views[viewName]
	e.mu.RUnlock()
	if !ok {
		return nil, false
	}
	return view.get(key)
}

// ViewRows returns all rows in a materialized view.
func (e *Engine) ViewRows(viewName string) ([]types.Row, bool) {
	e.mu.RLock()
	view, ok := e.views[viewName]
	e.mu.RUnlock()
	if !ok {
		return nil, false
	}
	return view.all(), true
}
