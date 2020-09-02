package dump

import "context"

type task struct {
	UID           int
	Limit, Offset int
	RunFunc       func(t *task) error
}

func (t task) ID() int {
	return t.UID
}

func (t *task) Run(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return nil
	default:
		return t.RunFunc(t)
	}
}
