package asynctask

import (
	"context"

	"github.com/cloudcarver/waitkit/pkg/model"
	"github.com/cloudcarver/waitkit/pkg/risingwave"
	counter "github.com/cloudcarver/waitkit/pkg/zgen/schemas/counter"
	"github.com/cloudcarver/waitkit/pkg/zgen/taskgen"

	"github.com/cloudcarver/anclax/pkg/taskcore/worker"
)

type Executor struct {
	model      model.ModelInterface
	risingwave risingwave.Client
}

func NewExecutor(model model.ModelInterface, risingwaveClient risingwave.Client) taskgen.ExecutorInterface {
	return &Executor{
		model:      model,
		risingwave: risingwaveClient,
	}
}

func (e *Executor) ExecuteIncrementCounter(ctx context.Context, _ worker.Task, params *counter.IncrementCounterParams) error {
	_ = params
	return e.model.IncrementCounter(ctx)
}

func (e *Executor) ExecuteAutoIncrementCounter(ctx context.Context, _ worker.Task, params *counter.IncrementCounterParams) error {
	_ = params
	return e.model.IncrementCounter(ctx)
}
