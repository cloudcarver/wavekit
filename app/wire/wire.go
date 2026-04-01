//go:build wireinject
// +build wireinject

package wire

import (
	"github.com/cloudcarver/waitkit/app"
	"github.com/cloudcarver/waitkit/pkg/config"
	"github.com/cloudcarver/waitkit/pkg/handler"
	"github.com/cloudcarver/waitkit/pkg/model"
	"github.com/cloudcarver/waitkit/pkg/risingwave"
	"github.com/cloudcarver/waitkit/pkg/service"

	"github.com/google/wire"
)

func InitApp() (*app.App, error) {
	wire.Build(
		app.InjectAuth,
		handler.NewHandler,
		handler.NewValidator,
		model.NewModel,
		config.NewConfig,
		risingwave.NewClient,
		service.NewService,
		app.Init,
		app.InitAnclaxApplication,
		app.NewPlugin,
	)
	return nil, nil
}
