package pool_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/partyzanex/repmy/pkg/pool"
)

func TestProcessPool_RunProcess(t *testing.T) {
	t.Parallel()

	p := &pool.ProcessPool{}
	errPool := &pool.ProcessPool{}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-time.After(3 * time.Second)
		cancel()
	}()

	params := pool.Params{
		Loop: true,
	}

	p.RunProcess(ctx, testProcess, &params)
	p.RunProcess(ctx, testProcess, nil)
	p.RunProcess(ctx, testProcess, nil)
	p.RunProcess(ctx, testProcess, nil)
	p.RunProcess(ctx, testProcess, nil)
	p.RunProcess(ctx, testProcess, nil)
	p.RunProcess(ctx, testProcess, nil)
	p.RunProcess(ctx, testProcess, nil)

	errPool.RunProcess(context.TODO(), func(ctx context.Context) error {
		errs := p.Errors()

		for err := range errs {
			if err != testErrDone {
				t.Fatal(err)
			}
		}

		return nil
	}, nil)

	p.Wait()
	errPool.Wait()
}

var (
	testErrDone = errors.New("expected error done")
)

func testProcess(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return testErrDone
		default:
			time.Sleep(50 * time.Millisecond)
		}
	}
}
