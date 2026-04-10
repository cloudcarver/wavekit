package handler

import (
	"errors"

	"github.com/cloudcarver/waitkit/pkg/service"
	"github.com/cloudcarver/waitkit/pkg/zgen/apigen"
	"github.com/gofiber/fiber/v3"
	"github.com/google/uuid"
)

type Handler struct {
	service service.ServiceInterface
}

func NewHandler(service service.ServiceInterface) (apigen.ServerInterface, error) {
	return &Handler{service: service}, nil
}

func (h *Handler) ConnectCluster(c fiber.Ctx) error {
	var req apigen.ConnectClusterRequest
	if err := c.Bind().Body(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).SendString(err.Error())
	}
	result, err := h.service.ConnectCluster(c.Context(), req)
	if err != nil {
		return writeServiceError(c, err)
	}
	return c.JSON(result)
}

func (h *Handler) ListClusters(c fiber.Ctx) error {
	result, err := h.service.ListClusters(c.Context())
	if err != nil {
		return writeServiceError(c, err)
	}
	return c.JSON(result)
}

func (h *Handler) ListClusterBackgroundProgress(c fiber.Ctx) error {
	result, err := h.service.ListClusterBackgroundProgress(c.Context())
	if err != nil {
		return writeServiceError(c, err)
	}
	return c.JSON(result)
}

func (h *Handler) UpdateCluster(c fiber.Ctx, clusterUuid uuid.UUID) error {
	var req apigen.UpdateClusterRequest
	if err := c.Bind().Body(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).SendString(err.Error())
	}
	result, err := h.service.UpdateCluster(c.Context(), clusterUuid, req)
	if err != nil {
		return writeServiceError(c, err)
	}
	return c.JSON(result)
}

func (h *Handler) DeleteCluster(c fiber.Ctx, clusterUuid uuid.UUID) error {
	if err := h.service.DeleteCluster(c.Context(), clusterUuid); err != nil {
		return writeServiceError(c, err)
	}
	return c.SendStatus(fiber.StatusNoContent)
}

func (h *Handler) ListClusterDatabases(c fiber.Ctx, clusterUuid uuid.UUID) error {
	result, err := h.service.ListClusterDatabases(c.Context(), clusterUuid)
	if err != nil {
		return writeServiceError(c, err)
	}
	return c.JSON(result)
}

func (h *Handler) ListClusterRelations(c fiber.Ctx, clusterUuid uuid.UUID, database string) error {
	result, err := h.service.ListClusterRelations(c.Context(), clusterUuid, database)
	if err != nil {
		return writeServiceError(c, err)
	}
	return c.JSON(result)
}

func (h *Handler) ExecuteClusterSql(c fiber.Ctx, clusterUuid uuid.UUID, database string) error {
	var req apigen.ExecuteSqlRequest
	if err := c.Bind().Body(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).SendString(err.Error())
	}
	result, err := h.service.ExecuteClusterSQL(c.Context(), clusterUuid, database, req)
	if err != nil {
		return writeServiceError(c, err)
	}
	return c.JSON(result)
}

func (h *Handler) ListBackgroundDdls(c fiber.Ctx) error {
	result, err := h.service.ListBackgroundDDLs(c.Context())
	if err != nil {
		return writeServiceError(c, err)
	}
	return c.JSON(result)
}

func (h *Handler) CreateBackgroundDdl(c fiber.Ctx) error {
	var req apigen.CreateBackgroundDdlRequest
	if err := c.Bind().Body(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).SendString(err.Error())
	}
	result, err := h.service.CreateBackgroundDDL(c.Context(), req)
	if err != nil {
		return writeServiceError(c, err)
	}
	return c.Status(fiber.StatusAccepted).JSON(result)
}

func (h *Handler) DeleteBackgroundDdl(c fiber.Ctx, id uuid.UUID) error {
	if err := h.service.DeleteBackgroundDDL(c.Context(), id); err != nil {
		return writeServiceError(c, err)
	}
	return c.SendStatus(fiber.StatusAccepted)
}

func (h *Handler) ListNotebooks(c fiber.Ctx) error {
	result, err := h.service.ListNotebooks(c.Context())
	if err != nil {
		return writeServiceError(c, err)
	}
	return c.JSON(result)
}

func (h *Handler) CreateNotebook(c fiber.Ctx) error {
	var req apigen.CreateNotebookRequest
	if err := c.Bind().Body(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).SendString(err.Error())
	}
	result, err := h.service.CreateNotebook(c.Context(), req)
	if err != nil {
		return writeServiceError(c, err)
	}
	return c.Status(fiber.StatusCreated).JSON(result)
}

func (h *Handler) GetNotebook(c fiber.Ctx, notebookUuid uuid.UUID) error {
	result, err := h.service.GetNotebook(c.Context(), notebookUuid)
	if err != nil {
		return writeServiceError(c, err)
	}
	return c.JSON(result)
}

func (h *Handler) DeleteNotebook(c fiber.Ctx, notebookUuid uuid.UUID) error {
	if err := h.service.DeleteNotebook(c.Context(), notebookUuid); err != nil {
		return writeServiceError(c, err)
	}
	return c.SendStatus(fiber.StatusNoContent)
}

func (h *Handler) CreateNotebookCell(c fiber.Ctx, notebookUuid uuid.UUID) error {
	var req apigen.CreateNotebookCellRequest
	if err := c.Bind().Body(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).SendString(err.Error())
	}
	result, err := h.service.CreateNotebookCell(c.Context(), notebookUuid, req)
	if err != nil {
		return writeServiceError(c, err)
	}
	return c.Status(fiber.StatusCreated).JSON(result)
}

func (h *Handler) UpdateNotebookCell(c fiber.Ctx, notebookUuid uuid.UUID, cellUuid uuid.UUID) error {
	var req apigen.UpdateNotebookCellRequest
	if err := c.Bind().Body(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).SendString(err.Error())
	}
	result, err := h.service.UpdateNotebookCell(c.Context(), notebookUuid, cellUuid, req)
	if err != nil {
		return writeServiceError(c, err)
	}
	return c.JSON(result)
}

func (h *Handler) DeleteNotebookCell(c fiber.Ctx, notebookUuid uuid.UUID, cellUuid uuid.UUID) error {
	if err := h.service.DeleteNotebookCell(c.Context(), notebookUuid, cellUuid); err != nil {
		return writeServiceError(c, err)
	}
	return c.SendStatus(fiber.StatusNoContent)
}

func (h *Handler) ReorderNotebookCells(c fiber.Ctx, notebookUuid uuid.UUID) error {
	var req apigen.ReorderNotebookCellsRequest
	if err := c.Bind().Body(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).SendString(err.Error())
	}
	if err := h.service.ReorderNotebookCells(c.Context(), notebookUuid, req); err != nil {
		return writeServiceError(c, err)
	}
	return c.SendStatus(fiber.StatusNoContent)
}

func writeServiceError(c fiber.Ctx, err error) error {
	switch {
	case errors.Is(err, service.ErrClusterNotFound), errors.Is(err, service.ErrNotebookNotFound), errors.Is(err, service.ErrNotebookCellNotFound), errors.Is(err, service.ErrBackgroundDdlNotFound):
		return c.Status(fiber.StatusNotFound).SendString(err.Error())
	case errors.Is(err, service.ErrInvalidInput), errors.Is(err, service.ErrInvalidCellOrder):
		return c.Status(fiber.StatusBadRequest).SendString(err.Error())
	default:
		return err
	}
}
