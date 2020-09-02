package pool_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/partyzanex/repmy/pkg/pool"
	"github.com/sirupsen/logrus"
)

func TestNewWorkersPool(t *testing.T) {
	t.Parallel()

	logrus.SetLevel(logrus.DebugLevel)
	result := int64(0)
	atomic.StoreInt64(&result, result)

	p := pool.NewWorkersPool(pool.Size(8))

	for i := 0; i < 1000; i++ {
		p.AddTask(&task{
			UID:    i,
			Result: &result,
		})
	}

	p.Wait()
	t.Log(result, atomic.LoadInt64(&result))
}

type task struct {
	UID    int
	Result *int64
}

func (t task) ID() int {
	return t.UID
}

func (t *task) Run(ctx context.Context) error {
	//logrus.Debugf("Task %d running", t.ID())
	//defer logrus.Debugf("Task %d finished", t.ID())

	atomic.AddInt64(t.Result, int64(t.UID))

	return nil
}

func TestSize(t *testing.T) {
	logrus.SetLevel(logrus.DebugLevel)

	p := &Pool{
		Size:    8,
		Workers: make(chan func()),
		Wg:      &sync.WaitGroup{},
		Mu:      &sync.Mutex{},
	}

	go p.Start()

	for i := 1; i <= 40; i++ {
		p.Add((func(id int) func() {
			return func() {
				logrus.Infof("Worker %d finished", id)
			}
		})(i))
	}

	logrus.Info("Wait 1")
	go p.Wait()

	logrus.Info("Wait 2")
	go p.Wait()

	logrus.Info("Wait 3")
	p.Wait()
}

type Pool struct {
	Size    int
	Workers chan func()
	Wg      *sync.WaitGroup
	Mu      *sync.Mutex
	Waited  bool
}

func (p *Pool) Add(fn func()) {
	p.Workers <- fn
}

func (p *Pool) Start() {
	logrus.Debug("start: pool started")
	defer logrus.Debug("start: starting finished")

	p.Wg.Add(p.Size)

	for i := 0; i < p.Size; i++ {
		go p.worker()
	}
}

func (p *Pool) worker() {
	defer p.Wg.Done()

	for worker := range p.Workers {
		worker()
	}
}

func (p *Pool) Wait() {
	p.Mu.Lock()
	defer p.Mu.Unlock()

	logrus.Debug("Wait: started")
	defer logrus.Debug("Wait: finished")

	if !p.Waited {
		p.Waited = true
		close(p.Workers)
		p.Wg.Wait()
	}
}
