package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/cloudcarver/waitkit/pkg/model"
	"github.com/cloudcarver/waitkit/pkg/risingwave"
	"github.com/cloudcarver/waitkit/pkg/zgen/apigen"
	"github.com/cloudcarver/waitkit/pkg/zgen/querier"
	"github.com/cloudcarver/waitkit/pkg/zgen/taskgen"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

var (
	ErrClusterNotFound       = errors.New("cluster not found")
	ErrNotebookNotFound      = errors.New("notebook not found")
	ErrNotebookCellNotFound  = errors.New("notebook cell not found")
	ErrBackgroundDdlNotFound = errors.New("background DDL job not found")
	ErrInvalidInput          = errors.New("invalid input")
	ErrInvalidCellOrder      = errors.New("invalid notebook cell order")
)

type ServiceInterface interface {
	ConnectCluster(ctx context.Context, req apigen.ConnectClusterRequest) (*apigen.ConnectClusterResult, error)
	ListClusters(ctx context.Context) (*apigen.ClusterList, error)
	UpdateCluster(ctx context.Context, clusterUUID uuid.UUID, req apigen.UpdateClusterRequest) (*apigen.Cluster, error)
	DeleteCluster(ctx context.Context, clusterUUID uuid.UUID) error
	ListClusterDatabases(ctx context.Context, clusterUUID uuid.UUID) (*apigen.ClusterDatabaseList, error)
	ListClusterRelations(ctx context.Context, clusterUUID uuid.UUID, database string) (*apigen.ClusterRelationList, error)
	ExecuteClusterSQL(ctx context.Context, clusterUUID uuid.UUID, database string, req apigen.ExecuteSqlRequest) (*apigen.SqlExecutionResult, error)
	ListClusterBackgroundProgress(ctx context.Context) (*apigen.BackgroundProgressList, error)
	CreateBackgroundDDL(ctx context.Context, req apigen.CreateBackgroundDdlRequest) (*apigen.CreateBackgroundDdlResult, error)
	ListBackgroundDDLs(ctx context.Context) (*apigen.BackgroundDdlList, error)
	DeleteBackgroundDDL(ctx context.Context, id uuid.UUID) error
	ListNotebooks(ctx context.Context) (*apigen.NotebookList, error)
	CreateNotebook(ctx context.Context, req apigen.CreateNotebookRequest) (*apigen.CreateNotebookResult, error)
	GetNotebook(ctx context.Context, notebookUUID uuid.UUID) (*apigen.Notebook, error)
	DeleteNotebook(ctx context.Context, notebookUUID uuid.UUID) error
	CreateNotebookCell(ctx context.Context, notebookUUID uuid.UUID, req apigen.CreateNotebookCellRequest) (*apigen.CreateNotebookCellResult, error)
	UpdateNotebookCell(ctx context.Context, notebookUUID uuid.UUID, cellUUID uuid.UUID, req apigen.UpdateNotebookCellRequest) (*apigen.NotebookCell, error)
	DeleteNotebookCell(ctx context.Context, notebookUUID uuid.UUID, cellUUID uuid.UUID) error
	ReorderNotebookCells(ctx context.Context, notebookUUID uuid.UUID, req apigen.ReorderNotebookCellsRequest) error
}

type Service struct {
	model      model.ModelInterface
	risingwave risingwave.Client
	taskRunner taskgen.TaskRunner
}

func NewService(model model.ModelInterface, risingwaveClient risingwave.Client) ServiceInterface {
	return NewServiceWithTaskRunner(model, risingwaveClient, nil)
}

func NewServiceWithTaskRunner(model model.ModelInterface, risingwaveClient risingwave.Client, taskRunner taskgen.TaskRunner) ServiceInterface {
	return &Service{
		model:      model,
		risingwave: risingwaveClient,
		taskRunner: taskRunner,
	}
}

func (s *Service) ConnectCluster(ctx context.Context, req apigen.ConnectClusterRequest) (*apigen.ConnectClusterResult, error) {
	if err := validateClusterRequest(req.ClusterName, req.SqlConnectionString); err != nil {
		return nil, err
	}
	metaNodeGrpcURL := optionalString(req.MetaNodeGrpcUrl)
	metaNodeHTTPURL := optionalString(req.MetaNodeHttpUrl)
	cluster, err := s.model.CreateCluster(ctx, querier.CreateClusterParams{
		ClusterName:         req.ClusterName,
		SqlConnectionString: req.SqlConnectionString,
		MetaNodeGrpcUrl:     metaNodeGrpcURL,
		MetaNodeHttpUrl:     metaNodeHTTPURL,
	})
	if err != nil {
		return nil, err
	}
	return &apigen.ConnectClusterResult{ClusterUuid: cluster.ClusterUuid}, nil
}

func (s *Service) ListClusters(ctx context.Context) (*apigen.ClusterList, error) {
	clusters, err := s.model.ListClusters(ctx)
	if err != nil {
		return nil, err
	}
	items := make([]apigen.Cluster, 0, len(clusters))
	for _, cluster := range clusters {
		items = append(items, *clusterToAPI(cluster, disabledConnectionStatus()))
	}
	return &apigen.ClusterList{Clusters: items}, nil
}

func (s *Service) UpdateCluster(ctx context.Context, clusterUUID uuid.UUID, req apigen.UpdateClusterRequest) (*apigen.Cluster, error) {
	if err := validateClusterRequest(req.ClusterName, req.SqlConnectionString); err != nil {
		return nil, err
	}
	if _, err := s.getCluster(ctx, clusterUUID); err != nil {
		return nil, err
	}
	metaNodeGrpcURL := optionalString(req.MetaNodeGrpcUrl)
	metaNodeHTTPURL := optionalString(req.MetaNodeHttpUrl)
	cluster, err := s.model.UpdateCluster(ctx, querier.UpdateClusterParams{
		ClusterUuid:         clusterUUID,
		ClusterName:         req.ClusterName,
		SqlConnectionString: req.SqlConnectionString,
		MetaNodeGrpcUrl:     metaNodeGrpcURL,
		MetaNodeHttpUrl:     metaNodeHTTPURL,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrClusterNotFound
		}
		return nil, err
	}
	return clusterToAPI(cluster, disabledConnectionStatus()), nil
}

func (s *Service) DeleteCluster(ctx context.Context, clusterUUID uuid.UUID) error {
	if _, err := s.getCluster(ctx, clusterUUID); err != nil {
		return err
	}
	return s.model.DeleteCluster(ctx, clusterUUID)
}

func (s *Service) ListClusterDatabases(ctx context.Context, clusterUUID uuid.UUID) (*apigen.ClusterDatabaseList, error) {
	cluster, err := s.getCluster(ctx, clusterUUID)
	if err != nil {
		return nil, err
	}
	databases, err := s.risingwave.ListDatabases(ctx, clusterToRWConfig(cluster))
	if err != nil {
		return nil, err
	}
	items := make([]apigen.DatabaseEntry, 0, len(databases))
	for _, database := range databases {
		items = append(items, apigen.DatabaseEntry{Name: database})
	}
	return &apigen.ClusterDatabaseList{Databases: items}, nil
}

func (s *Service) ListClusterRelations(ctx context.Context, clusterUUID uuid.UUID, database string) (*apigen.ClusterRelationList, error) {
	if strings.TrimSpace(database) == "" {
		return nil, fmt.Errorf("%w: database is required", ErrInvalidInput)
	}
	cluster, err := s.getCluster(ctx, clusterUUID)
	if err != nil {
		return nil, err
	}
	categories, err := s.risingwave.ListRelations(ctx, clusterToRWConfig(cluster), database)
	if err != nil {
		return nil, err
	}
	items := make([]apigen.RelationCategory, 0, len(categories))
	for _, category := range categories {
		categoryItem := apigen.RelationCategory{
			Name:    category.Name,
			Schemas: make([]apigen.RelationSchema, 0, len(category.Schemas)),
		}
		for _, schema := range category.Schemas {
			schemaItem := apigen.RelationSchema{
				SchemaName: schema.SchemaName,
				Relations:  make([]apigen.Relation, 0, len(schema.Relations)),
			}
			for _, relation := range schema.Relations {
				relationItem := apigen.Relation{
					SchemaName:   relation.SchemaName,
					RelationName: relation.RelationName,
					RelationType: relation.RelationType,
					Columns:      make([]apigen.RelationColumn, 0, len(relation.Columns)),
				}
				for _, column := range relation.Columns {
					relationItem.Columns = append(relationItem.Columns, apigen.RelationColumn{
						Name:     column.Name,
						DataType: column.DataType,
					})
				}
				schemaItem.Relations = append(schemaItem.Relations, relationItem)
			}
			categoryItem.Schemas = append(categoryItem.Schemas, schemaItem)
		}
		items = append(items, categoryItem)
	}
	return &apigen.ClusterRelationList{Categories: items}, nil
}

func (s *Service) ExecuteClusterSQL(ctx context.Context, clusterUUID uuid.UUID, database string, req apigen.ExecuteSqlRequest) (*apigen.SqlExecutionResult, error) {
	if strings.TrimSpace(database) == "" || strings.TrimSpace(req.Statement) == "" {
		return nil, fmt.Errorf("%w: database and statement are required", ErrInvalidInput)
	}
	cluster, err := s.getCluster(ctx, clusterUUID)
	if err != nil {
		return nil, err
	}
	statement := injectSelectLimit(req.Statement)
	if req.BackgroundDDL != nil && *req.BackgroundDDL {
		statement = "SET BACKGROUND_DDL=true;\n" + statement
	}
	result, err := s.risingwave.ExecuteSQL(ctx, clusterToRWConfig(cluster), database, statement)
	if err != nil {
		return sqlExecutionErrorResult(err), nil
	}
	apiResult := sqlResultToAPI(result)
	return &apiResult, nil
}

func (s *Service) ListClusterBackgroundProgress(ctx context.Context) (*apigen.BackgroundProgressList, error) {
	clusters, err := s.model.ListClusters(ctx)
	if err != nil {
		return nil, err
	}

	items := make([]apigen.BackgroundProgressCluster, 0, len(clusters))
	for _, cluster := range clusters {
		clusterItem := apigen.BackgroundProgressCluster{
			ClusterUuid: cluster.ClusterUuid,
			ClusterName: cluster.ClusterName,
			Databases:   []apigen.BackgroundProgressDatabase{},
		}

		databaseCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		databases, dbErr := s.risingwave.ListDatabases(databaseCtx, clusterToRWConfig(cluster))
		cancel()
		if dbErr != nil {
			message := dbErr.Error()
			clusterItem.Error = &message
			items = append(items, clusterItem)
			continue
		}

		for _, database := range databases {
			ddl := s.queryBackgroundProgress(ctx, cluster, database, "SELECT * FROM rw_ddl_progress LIMIT 100")
			cdc := s.queryBackgroundProgress(ctx, cluster, database, "SELECT * FROM rw_cdc_progress LIMIT 100")
			clusterItem.Databases = append(clusterItem.Databases, apigen.BackgroundProgressDatabase{
				Database: database,
				Ddl:      ddl,
				Cdc:      cdc,
			})
		}

		items = append(items, clusterItem)
	}

	return &apigen.BackgroundProgressList{Clusters: items}, nil
}

func (s *Service) ListNotebooks(ctx context.Context) (*apigen.NotebookList, error) {
	notebooks, err := s.model.ListNotebooks(ctx)
	if err != nil {
		return nil, err
	}
	items := make([]apigen.NotebookSummary, 0, len(notebooks))
	for _, notebook := range notebooks {
		items = append(items, apigen.NotebookSummary{
			NotebookUuid: notebook.NotebookUuid,
			NotebookName: notebook.NotebookName,
			UpdatedAt:    notebook.UpdatedAt,
		})
	}
	return &apigen.NotebookList{Notebooks: items}, nil
}

func (s *Service) CreateNotebook(ctx context.Context, req apigen.CreateNotebookRequest) (*apigen.CreateNotebookResult, error) {
	if strings.TrimSpace(req.NotebookName) == "" {
		return nil, fmt.Errorf("%w: notebookName is required", ErrInvalidInput)
	}
	notebook, err := s.model.CreateNotebook(ctx, req.NotebookName)
	if err != nil {
		return nil, err
	}
	return &apigen.CreateNotebookResult{NotebookUuid: notebook.NotebookUuid}, nil
}

func (s *Service) GetNotebook(ctx context.Context, notebookUUID uuid.UUID) (*apigen.Notebook, error) {
	notebook, err := s.getNotebook(ctx, notebookUUID)
	if err != nil {
		return nil, err
	}
	cells, err := s.model.ListNotebookCells(ctx, notebookUUID)
	if err != nil {
		return nil, err
	}
	return &apigen.Notebook{
		NotebookUuid: notebook.NotebookUuid,
		NotebookName: notebook.NotebookName,
		UpdatedAt:    notebook.UpdatedAt,
		Cells:        notebookCellsToAPI(cells),
	}, nil
}

func (s *Service) DeleteNotebook(ctx context.Context, notebookUUID uuid.UUID) error {
	if _, err := s.getNotebook(ctx, notebookUUID); err != nil {
		return err
	}
	return s.model.DeleteNotebook(ctx, notebookUUID)
}

func (s *Service) CreateNotebookCell(ctx context.Context, notebookUUID uuid.UUID, req apigen.CreateNotebookCellRequest) (*apigen.CreateNotebookCellResult, error) {
	if err := validateNotebookCellInput(req.CellType, req.Content); err != nil {
		return nil, err
	}
	if _, err := s.getNotebook(ctx, notebookUUID); err != nil {
		return nil, err
	}
	if err := s.validateClusterContext(ctx, req.ClusterUuid); err != nil {
		return nil, err
	}
	orderIndex, err := s.model.GetNextNotebookCellOrder(ctx, notebookUUID)
	if err != nil {
		return nil, err
	}
	cell, err := s.model.CreateNotebookCell(ctx, querier.CreateNotebookCellParams{
		NotebookUuid:  notebookUUID,
		CellType:      string(req.CellType),
		ClusterUuid:   toNullUUID(req.ClusterUuid),
		DatabaseName:  req.Database,
		BackgroundDdl: optionalBool(req.BackgroundDdl),
		Content:       req.Content,
		OrderIndex:    orderIndex,
	})
	if err != nil {
		return nil, err
	}
	if err := s.model.UpdateNotebookTimestamp(ctx, notebookUUID); err != nil {
		return nil, err
	}
	return &apigen.CreateNotebookCellResult{CellUuid: cell.CellUuid}, nil
}

func (s *Service) UpdateNotebookCell(ctx context.Context, notebookUUID uuid.UUID, cellUUID uuid.UUID, req apigen.UpdateNotebookCellRequest) (*apigen.NotebookCell, error) {
	if err := validateNotebookCellInput(req.CellType, req.Content); err != nil {
		return nil, err
	}
	if _, err := s.getNotebook(ctx, notebookUUID); err != nil {
		return nil, err
	}
	if err := s.validateClusterContext(ctx, req.ClusterUuid); err != nil {
		return nil, err
	}
	cell, err := s.model.UpdateNotebookCell(ctx, querier.UpdateNotebookCellParams{
		NotebookUuid:  notebookUUID,
		CellUuid:      cellUUID,
		CellType:      string(req.CellType),
		ClusterUuid:   toNullUUID(req.ClusterUuid),
		DatabaseName:  req.Database,
		BackgroundDdl: optionalBool(req.BackgroundDdl),
		Content:       req.Content,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotebookCellNotFound
		}
		return nil, err
	}
	if err := s.model.UpdateNotebookTimestamp(ctx, notebookUUID); err != nil {
		return nil, err
	}
	apiCells := notebookCellsToAPI([]*querier.NotebookCell{cell})
	return &apiCells[0], nil
}

func (s *Service) DeleteNotebookCell(ctx context.Context, notebookUUID uuid.UUID, cellUUID uuid.UUID) error {
	if _, err := s.model.GetNotebookCell(ctx, querier.GetNotebookCellParams{NotebookUuid: notebookUUID, CellUuid: cellUUID}); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotebookCellNotFound
		}
		return err
	}
	if err := s.model.DeleteNotebookCell(ctx, querier.DeleteNotebookCellParams{NotebookUuid: notebookUUID, CellUuid: cellUUID}); err != nil {
		return err
	}
	return s.model.UpdateNotebookTimestamp(ctx, notebookUUID)
}

func (s *Service) ReorderNotebookCells(ctx context.Context, notebookUUID uuid.UUID, req apigen.ReorderNotebookCellsRequest) error {
	if _, err := s.getNotebook(ctx, notebookUUID); err != nil {
		return err
	}
	currentCells, err := s.model.ListNotebookCells(ctx, notebookUUID)
	if err != nil {
		return err
	}
	if len(currentCells) != len(req.CellUuids) {
		return ErrInvalidCellOrder
	}
	remaining := map[uuid.UUID]struct{}{}
	var maxOrderIndex int32 = -1
	for _, cell := range currentCells {
		remaining[cell.CellUuid] = struct{}{}
		if cell.OrderIndex > maxOrderIndex {
			maxOrderIndex = cell.OrderIndex
		}
	}
	for _, cellUUID := range req.CellUuids {
		if _, ok := remaining[cellUUID]; !ok {
			return ErrInvalidCellOrder
		}
		delete(remaining, cellUUID)
	}
	if len(remaining) != 0 {
		return ErrInvalidCellOrder
	}

	return s.model.RunTransaction(ctx, func(txModel model.ModelInterface) error {
		for index, cellUUID := range req.CellUuids {
			if err := txModel.UpdateNotebookCellOrder(ctx, querier.UpdateNotebookCellOrderParams{
				NotebookUuid: notebookUUID,
				CellUuid:     cellUUID,
				OrderIndex:   maxOrderIndex + int32(index) + 1,
			}); err != nil {
				return err
			}
		}
		for orderIndex, cellUUID := range req.CellUuids {
			if err := txModel.UpdateNotebookCellOrder(ctx, querier.UpdateNotebookCellOrderParams{
				NotebookUuid: notebookUUID,
				CellUuid:     cellUUID,
				OrderIndex:   int32(orderIndex),
			}); err != nil {
				return err
			}
		}
		return txModel.UpdateNotebookTimestamp(ctx, notebookUUID)
	})
}

func (s *Service) getCluster(ctx context.Context, clusterUUID uuid.UUID) (*querier.Cluster, error) {
	cluster, err := s.model.GetCluster(ctx, clusterUUID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrClusterNotFound
		}
		return nil, err
	}
	return cluster, nil
}

func (s *Service) getNotebook(ctx context.Context, notebookUUID uuid.UUID) (*querier.Notebook, error) {
	notebook, err := s.model.GetNotebook(ctx, notebookUUID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotebookNotFound
		}
		return nil, err
	}
	return notebook, nil
}

func (s *Service) validateClusterContext(ctx context.Context, clusterUUID *uuid.UUID) error {
	if clusterUUID == nil {
		return nil
	}
	_, err := s.getCluster(ctx, *clusterUUID)
	return err
}

func validateClusterRequest(clusterName string, sqlConnectionString string) error {
	if strings.TrimSpace(clusterName) == "" || strings.TrimSpace(sqlConnectionString) == "" {
		return fmt.Errorf("%w: clusterName and sqlConnectionString are required", ErrInvalidInput)
	}
	return nil
}

func optionalString(value *string) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace(*value)
}

func optionalBool(value *bool) bool {
	if value == nil {
		return false
	}
	return *value
}

func validateNotebookCellInput(cellType apigen.NotebookCellType, content string) error {
	switch cellType {
	case apigen.NotebookCellTypeSQL, apigen.NotebookCellTypeShell:
	default:
		return fmt.Errorf("%w: unsupported cellType %q", ErrInvalidInput, cellType)
	}
	if strings.TrimSpace(content) == "" {
		return fmt.Errorf("%w: content is required", ErrInvalidInput)
	}
	return nil
}

func toNullUUID(value *uuid.UUID) uuid.NullUUID {
	if value == nil {
		return uuid.NullUUID{}
	}
	return uuid.NullUUID{UUID: *value, Valid: true}
}

func clusterToRWConfig(cluster *querier.Cluster) risingwave.ClusterConfig {
	return risingwave.ClusterConfig{
		SQLConnectionString: cluster.SqlConnectionString,
		MetaNodeGrpcURL:     cluster.MetaNodeGrpcUrl,
		MetaNodeHTTPURL:     cluster.MetaNodeHttpUrl,
	}
}

func clusterToAPI(cluster *querier.Cluster, status *risingwave.ConnectionStatus) *apigen.Cluster {
	return &apigen.Cluster{
		ClusterUuid:         cluster.ClusterUuid,
		ClusterName:         cluster.ClusterName,
		SqlConnectionString: cluster.SqlConnectionString,
		MetaNodeGrpcUrl:     cluster.MetaNodeGrpcUrl,
		MetaNodeHttpUrl:     cluster.MetaNodeHttpUrl,
		ConnectionStatus:    connectionStatusToAPI(status),
	}
}

func (s *Service) queryBackgroundProgress(ctx context.Context, cluster *querier.Cluster, database string, statement string) apigen.SqlExecutionResult {
	queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	result, err := s.risingwave.ExecuteSQL(queryCtx, clusterToRWConfig(cluster), database, statement)
	if err != nil {
		return *sqlExecutionErrorResult(err)
	}
	return sqlResultToAPI(result)
}

func sqlExecutionErrorResult(err error) *apigen.SqlExecutionResult {
	errMessage := err.Error()
	return &apigen.SqlExecutionResult{
		Columns:      []string{},
		Rows:         []apigen.SqlRow{},
		CommandTag:   "",
		RowsAffected: 0,
		Error:        &errMessage,
	}
}

func sqlResultToAPI(result *risingwave.SQLResult) apigen.SqlExecutionResult {
	columns := result.Columns
	if columns == nil {
		columns = []string{}
	}
	rows := make([]apigen.SqlRow, 0, len(result.Rows))
	for _, row := range result.Rows {
		values := make([]apigen.SqlValue, 0, len(row))
		for _, value := range row {
			values = append(values, apigen.SqlValue(value))
		}
		rows = append(rows, apigen.SqlRow{Values: values})
	}
	return apigen.SqlExecutionResult{
		Columns:      columns,
		Rows:         rows,
		CommandTag:   result.CommandTag,
		RowsAffected: result.RowsAffected,
	}
}

func disabledConnectionStatus() *risingwave.ConnectionStatus {
	return &risingwave.ConnectionStatus{
		CheckedAt: time.Now().UTC(),
		SQL:       risingwave.EndpointCheck{OK: false, Message: "validation disabled"},
		Meta:      risingwave.EndpointCheck{OK: false, Message: "validation disabled"},
	}
}

func connectionStatusToAPI(status *risingwave.ConnectionStatus) apigen.ConnectionStatus {
	if status == nil {
		status = disabledConnectionStatus()
	}
	return apigen.ConnectionStatus{
		Sql: apigen.EndpointCheck{
			Ok:      status.SQL.OK,
			Message: status.SQL.Message,
		},
		Meta: apigen.EndpointCheck{
			Ok:      status.Meta.OK,
			Message: status.Meta.Message,
		},
		CheckedAt: status.CheckedAt,
	}
}

func notebookCellsToAPI(cells []*querier.NotebookCell) []apigen.NotebookCell {
	items := make([]apigen.NotebookCell, 0, len(cells))
	for _, cell := range cells {
		item := apigen.NotebookCell{
			CellUuid:      cell.CellUuid,
			CellType:      apigen.NotebookCellType(cell.CellType),
			BackgroundDdl: cell.BackgroundDdl,
			Content:       cell.Content,
			OrderIndex:    cell.OrderIndex,
			UpdatedAt:     cell.UpdatedAt,
		}
		if cell.ClusterUuid.Valid {
			clusterUUID := cell.ClusterUuid.UUID
			item.ClusterUuid = &clusterUUID
		}
		if cell.DatabaseName != nil {
			database := *cell.DatabaseName
			item.Database = &database
		}
		items = append(items, item)
	}
	return items
}
