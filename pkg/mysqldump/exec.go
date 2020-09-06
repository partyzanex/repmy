package mysqldump

import (
	"bytes"
	"context"
	"github.com/partyzanex/repmy/pkg/pool"
	"github.com/pkg/errors"
	"io"
	"os/exec"
	"strings"
)

type executor struct {
	args   []string
	writer io.Writer
}

func (c *executor) ID() interface{} {
	return "mysqldump " + strings.Join(c.args, " ")
}

func (c *executor) Run(ctx context.Context) error {
	cmd := exec.CommandContext(ctx, "mysqldump", c.args...)

	stdout := c.writer
	stderr := &bytes.Buffer{}

	cmd.Stdout = stdout
	cmd.Stderr = stderr

	err := cmd.Start()
	if err != nil {
		return errors.Wrapf(err, "unable to start mysqldump with args: %v", cmd.Args)
	}

	errs := make(chan error)
	go func() {
		errs <- cmd.Wait()
	}()

	select {
	case <-ctx.Done():
		err := cmd.Process.Kill()
		if err != nil {
			return errors.Wrap(err, "killing process failed when context canceled")
		}
	case err := <-errs:
		if stderr.Len() > 0 {
			if err != nil {
				err = errors.Wrapf(err, stderr.String())
			} else {
				err = errors.New(stderr.String())
			}
		}

		if err != nil {
			return errors.Wrap(err, "executing mysqldump failed")
		}
	}

	return nil
}

func NewExecutor(w io.Writer, args ...string) pool.Task {
	return &executor{
		writer: w,
		args:   args,
	}
}
