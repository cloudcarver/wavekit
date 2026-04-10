//go:build wireinject
// +build wireinject

package wire

import (
	"github.com/cloudcarver/waitkit/app"
	"github.com/cloudcarver/waitkit/pkg/asynctask"
	"github.com/cloudcarver/waitkit/pkg/config"
	"github.com/cloudcarver/waitkit/pkg/handler"
	"github.com/cloudcarver/waitkit/pkg/model"
	"github.com/cloudcarver/waitkit/pkg/risingwave"
	"github.com/cloudcarver/waitkit/pkg/service"
	"github.com/cloudcarver/waitkit/pkg/zgen/taskgen"

	"github.com/google/wire"
)

func InitApp() (*app.App, error) {
	wire.Build(
		app.InjectAuth,
		app.InjectTaskStore,
		handler.NewHandler,
		handler.NewValidator,
		model.NewModel,
		config.NewConfig,
		risingwave.NewClient,
		service.NewServiceWithTaskRunner,
		taskgen.NewTaskRunner,
		asynctask.NewExecutor,
		taskgen.NewTaskHandler,
		app.Init,
		app.InitAnclaxApplication,
		app.NewPlugin,
	)
	return nil, nil
}
