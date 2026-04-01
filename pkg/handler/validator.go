package handler

import (
	"github.com/cloudcarver/waitkit/pkg/zgen/apigen"

	"github.com/cloudcarver/anclax/pkg/auth"
	"github.com/gofiber/fiber/v3"
)

type Validator struct {
	auth auth.AuthInterface
}

func NewValidator(auth auth.AuthInterface) apigen.Validator {
	return &Validator{auth: auth}
}

func (v *Validator) AuthFunc(c fiber.Ctx) error {
	_ = c
	return nil
}

func (v *Validator) PreValidate(c fiber.Ctx) error {
	_ = c
	return nil
}

func (v *Validator) PostValidate(c fiber.Ctx) error {
	_ = c
	return nil
}

func (v *Validator) OperationPermit(c fiber.Ctx, operationID string) error {
	_, _ = c, operationID
	return nil
}
