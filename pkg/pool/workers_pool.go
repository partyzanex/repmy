package pool

import (
	"context"
	"sync"

	"github.com/sirupsen/logrus"
)

type Option func(p *WorkersPool)

func WithCtx(ctx context.Context) Option {
	return func(p *WorkersPool) {
		p.ctx = ctx
	}
}

func Size(size int) Option {
	return func(p *WorkersPool) {
		p.size = size
	}
}

type Task interface {
	Run(ctx context.Context) error
	ID() int
}

type WorkersPool struct {
	ctx context.Context

	size  int
	tasks chan Task
	errs  chan error

	wg *sync.WaitGroup
	mu *sync.Mutex
}

func (p *WorkersPool) AddTask(task Task) {
	logrus.Debugf("AddTask: task %d adding", task.ID())
	defer logrus.Debugf("AddTask: task %d added", task.ID())

	p.tasks <- task
}

func (p *WorkersPool) start() {
	logrus.Debug("start: pool started")
	defer logrus.Debug("start: starting finished")

	p.wg.Add(p.size)

	if p.ctx == nil {
		p.ctx = context.Background()
	}

	for i := 0; i < p.size; i++ {
		go p.worker(i)
	}
}

func (p *WorkersPool) worker(n int) {
	logrus.Debugf("worker %d: started", n)
	defer logrus.Debugf("worker %d: finished", n)

	defer p.wg.Done()

	for task := range p.tasks {
		err := task.Run(p.ctx)
		if err != nil {
			select {
			case p.errs <- err:
				logrus.WithField("taskID:", task.ID()).Debug(err)
			default:
			}
		}
	}
}

func (p *WorkersPool) Wait() {
	// lock
	p.mu.Lock()
	defer p.mu.Unlock()

	logrus.Debug("Wait: started")
	defer logrus.Debug("Wait: finished")

	close(p.tasks)

	p.wg.Wait()

	close(p.errs)
}

const (
	DefaultPoolSize = 8
)

func NewWorkersPool(options ...Option) *WorkersPool {
	p := &WorkersPool{
		size:  DefaultPoolSize,
		tasks: make(chan Task),
		errs:  make(chan error),
		wg:    &sync.WaitGroup{},
		mu:    &sync.Mutex{},
	}

	for _, option := range options {
		option(p)
	}

	go p.start()

	return p
}
