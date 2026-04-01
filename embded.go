package root

import (
	"embed"
	"io/fs"

	"github.com/gofiber/fiber/v3"
	fiberStatic "github.com/gofiber/fiber/v3/middleware/static"
)

//go:embed sql/migrations/*
var Migrations embed.FS

//go:embed all:web/out
var WebStatic embed.FS // go to web folder, run pnpm install && pnpm build

func PlugWebStatic(app *fiber.App) error {
	webStatic, err := fs.Sub(WebStatic, "web/out")
	if err != nil {
		return err
	}

	app.Use("/", fiberStatic.New("", fiberStatic.Config{
		FS: webStatic,
	}))

	return nil
}
