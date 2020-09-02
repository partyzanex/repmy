package pool

import (
	"context"
	"sync"
	"time"
)

// params for running processes
type Params struct {
	// restart process when returns error
	Loop bool
	// delay between process restarts
	Delay time.Duration
}

type Process func(ctx context.Context) error

type ProcessPool struct {
	wg   *sync.WaitGroup
	errs chan error
}

func (p *ProcessPool) RunProcess(ctx context.Context, process Process, params *Params) {
	if p.wg == nil {
		p.wg = &sync.WaitGroup{}
	}

	p.wg.Add(1)

	go func() {
		defer p.wg.Done()

		if params == nil || !params.Loop {
			err := process(ctx)
			if p.errs != nil && err != nil {
				p.errs <- err
			}

			return
		}

		for {
			err := process(ctx)
			if p.errs != nil && err != nil {
				p.errs <- err
			}

			select {
			case <-ctx.Done():
				return
			default:
			}

			if params.Delay > 0 {
				time.Sleep(params.Delay)
			}
		}
	}()
}

func (p *ProcessPool) Errors() <-chan error {
	if p.errs == nil {
		p.errs = make(chan error)
	}

	return p.errs
}

func (p *ProcessPool) Wait() {
	if p.wg != nil {
		p.wg.Wait()
	}

	if p.errs != nil {
		close(p.errs)
	}
}
