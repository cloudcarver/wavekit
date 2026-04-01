package service

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cloudcarver/waitkit/pkg/model"
	"github.com/cloudcarver/waitkit/pkg/risingwave"
	"github.com/cloudcarver/waitkit/pkg/zgen/apigen"
	"github.com/cloudcarver/waitkit/pkg/zgen/querier"
	"github.com/google/uuid"
	"go.uber.org/mock/gomock"
)

type stubRisingWaveClient struct {
	validateCluster func(ctx context.Context, cfg risingwave.ClusterConfig) (*risingwave.ConnectionStatus, error)
	listDatabases   func(ctx context.Context, cfg risingwave.ClusterConfig) ([]string, error)
	listRelations   func(ctx context.Context, cfg risingwave.ClusterConfig, database string) ([]risingwave.RelationCategory, error)
	executeSQL      func(ctx context.Context, cfg risingwave.ClusterConfig, database, statement string) (*risingwave.SQLResult, error)
}

func ptr(value string) *string {
	return &value
}

func (s stubRisingWaveClient) ValidateCluster(ctx context.Context, cfg risingwave.ClusterConfig) (*risingwave.ConnectionStatus, error) {
	if s.validateCluster == nil {
		return &risingwave.ConnectionStatus{}, nil
	}
	return s.validateCluster(ctx, cfg)
}

func (s stubRisingWaveClient) ListDatabases(ctx context.Context, cfg risingwave.ClusterConfig) ([]string, error) {
	if s.listDatabases == nil {
		return nil, nil
	}
	return s.listDatabases(ctx, cfg)
}

func (s stubRisingWaveClient) ListRelations(ctx context.Context, cfg risingwave.ClusterConfig, database string) ([]risingwave.RelationCategory, error) {
	if s.listRelations == nil {
		return nil, nil
	}
	return s.listRelations(ctx, cfg, database)
}

func (s stubRisingWaveClient) ExecuteSQL(ctx context.Context, cfg risingwave.ClusterConfig, database, statement string) (*risingwave.SQLResult, error) {
	if s.executeSQL == nil {
		return nil, nil
	}
	return s.executeSQL(ctx, cfg, database, statement)
}

func TestConnectClusterSkipsValidation(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockModel := model.NewMockModelInterface(ctrl)
	clusterID := uuid.New()
	mockModel.EXPECT().CreateCluster(gomock.Any(), querier.CreateClusterParams{
		ClusterName:         "dev",
		SqlConnectionString: "postgres://localhost:4566/dev",
		MetaNodeGrpcUrl:     "",
		MetaNodeHttpUrl:     "",
	}).Return(&querier.Cluster{ClusterUuid: clusterID}, nil)

	svc := NewService(mockModel, stubRisingWaveClient{
		validateCluster: func(ctx context.Context, cfg risingwave.ClusterConfig) (*risingwave.ConnectionStatus, error) {
			t.Fatal("ValidateCluster should not be called")
			return nil, nil
		},
	})

	result, err := svc.ConnectCluster(context.Background(), apigen.ConnectClusterRequest{
		ClusterName:         "dev",
		SqlConnectionString: "postgres://localhost:4566/dev",
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if result.ClusterUuid != clusterID {
		t.Fatalf("unexpected cluster uuid: %s", result.ClusterUuid)
	}
}

func TestConnectClusterPersistsMetaEndpointsWithoutValidation(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockModel := model.NewMockModelInterface(ctrl)
	clusterID := uuid.New()
	mockModel.EXPECT().CreateCluster(gomock.Any(), querier.CreateClusterParams{
		ClusterName:         "dev",
		SqlConnectionString: "postgres://localhost:4566/dev",
		MetaNodeGrpcUrl:     "http://127.0.0.1:5690",
		MetaNodeHttpUrl:     "http://127.0.0.1:5691",
	}).Return(&querier.Cluster{ClusterUuid: clusterID}, nil)

	svc := NewService(mockModel, stubRisingWaveClient{
		validateCluster: func(ctx context.Context, cfg risingwave.ClusterConfig) (*risingwave.ConnectionStatus, error) {
			t.Fatal("ValidateCluster should not be called")
			return nil, nil
		},
	})

	result, err := svc.ConnectCluster(context.Background(), apigen.ConnectClusterRequest{
		ClusterName:         "dev",
		SqlConnectionString: "postgres://localhost:4566/dev",
		MetaNodeGrpcUrl:     ptr("http://127.0.0.1:5690"),
		MetaNodeHttpUrl:     ptr("http://127.0.0.1:5691"),
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if result.ClusterUuid != clusterID {
		t.Fatalf("unexpected cluster uuid: %s", result.ClusterUuid)
	}
}

func TestListClustersReturnsDisabledConnectionStatus(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockModel := model.NewMockModelInterface(ctrl)
	clusterID := uuid.New()
	mockModel.EXPECT().ListClusters(gomock.Any()).Return([]*querier.Cluster{
		{
			ClusterUuid:         clusterID,
			ClusterName:         "dev",
			SqlConnectionString: "postgres://localhost:4566/dev",
			MetaNodeGrpcUrl:     "",
			MetaNodeHttpUrl:     "",
			UpdatedAt:           time.Now().UTC(),
		},
	}, nil)

	svc := NewService(mockModel, stubRisingWaveClient{
		validateCluster: func(ctx context.Context, cfg risingwave.ClusterConfig) (*risingwave.ConnectionStatus, error) {
			t.Fatal("ValidateCluster should not be called")
			return nil, nil
		},
	})

	result, err := svc.ListClusters(context.Background())
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(result.Clusters) != 1 {
		t.Fatalf("expected 1 cluster, got %d", len(result.Clusters))
	}
	if result.Clusters[0].ConnectionStatus.Sql.Message != "validation disabled" {
		t.Fatalf("unexpected sql status: %#v", result.Clusters[0].ConnectionStatus.Sql)
	}
	if result.Clusters[0].ConnectionStatus.Meta.Message != "validation disabled" {
		t.Fatalf("unexpected meta status: %#v", result.Clusters[0].ConnectionStatus.Meta)
	}
}

func TestUpdateClusterSkipsValidation(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockModel := model.NewMockModelInterface(ctrl)
	clusterID := uuid.New()
	mockModel.EXPECT().GetCluster(gomock.Any(), clusterID).Return(&querier.Cluster{
		ClusterUuid:         clusterID,
		ClusterName:         "old",
		SqlConnectionString: "postgres://localhost:4566/dev",
		MetaNodeGrpcUrl:     "",
		MetaNodeHttpUrl:     "",
	}, nil)
	mockModel.EXPECT().UpdateCluster(gomock.Any(), querier.UpdateClusterParams{
		ClusterUuid:         clusterID,
		ClusterName:         "new",
		SqlConnectionString: "postgres://localhost:4566/dev",
		MetaNodeGrpcUrl:     "",
		MetaNodeHttpUrl:     "",
	}).Return(&querier.Cluster{
		ClusterUuid:         clusterID,
		ClusterName:         "new",
		SqlConnectionString: "postgres://localhost:4566/dev",
		MetaNodeGrpcUrl:     "",
		MetaNodeHttpUrl:     "",
	}, nil)

	svc := NewService(mockModel, stubRisingWaveClient{
		validateCluster: func(ctx context.Context, cfg risingwave.ClusterConfig) (*risingwave.ConnectionStatus, error) {
			t.Fatal("ValidateCluster should not be called")
			return nil, nil
		},
	})

	result, err := svc.UpdateCluster(context.Background(), clusterID, apigen.UpdateClusterRequest{
		ClusterName:         "new",
		SqlConnectionString: "postgres://localhost:4566/dev",
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if result.ConnectionStatus.Sql.Message != "validation disabled" {
		t.Fatalf("unexpected sql status: %#v", result.ConnectionStatus.Sql)
	}
}

func TestListClusterBackgroundProgressMapsResultsAndErrors(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockModel := model.NewMockModelInterface(ctrl)
	clusterID := uuid.New()
	mockModel.EXPECT().ListClusters(gomock.Any()).Return([]*querier.Cluster{
		{
			ClusterUuid:         clusterID,
			ClusterName:         "dev",
			SqlConnectionString: "postgres://localhost:4566/dev",
			MetaNodeGrpcUrl:     "",
			MetaNodeHttpUrl:     "",
		},
	}, nil)

	var statements []string
	svc := NewService(mockModel, stubRisingWaveClient{
		listDatabases: func(ctx context.Context, cfg risingwave.ClusterConfig) ([]string, error) {
			return []string{"dev"}, nil
		},
		executeSQL: func(ctx context.Context, cfg risingwave.ClusterConfig, database, statement string) (*risingwave.SQLResult, error) {
			statements = append(statements, statement)
			if statement == "SELECT * FROM rw_cdc_progress LIMIT 100" {
				return nil, fmt.Errorf("relation \"rw_cdc_progress\" does not exist")
			}
			return &risingwave.SQLResult{
				Columns:      []string{"id", "statement"},
				Rows:         [][]string{{"1", "create table t (id int)"}},
				CommandTag:   "SELECT 1",
				RowsAffected: 1,
			}, nil
		},
	})

	result, err := svc.ListClusterBackgroundProgress(context.Background())
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(result.Clusters) != 1 {
		t.Fatalf("expected 1 cluster, got %d", len(result.Clusters))
	}
	if len(result.Clusters[0].Databases) != 1 {
		t.Fatalf("expected 1 database, got %d", len(result.Clusters[0].Databases))
	}
	if len(result.Clusters[0].Databases[0].Ddl.Rows) != 1 {
		t.Fatalf("expected ddl progress rows, got %#v", result.Clusters[0].Databases[0].Ddl)
	}
	if result.Clusters[0].Databases[0].Cdc.Error == nil {
		t.Fatalf("expected cdc error, got %#v", result.Clusters[0].Databases[0].Cdc)
	}
	if len(statements) != 2 {
		t.Fatalf("expected 2 progress queries, got %d", len(statements))
	}
}

func TestListClusterRelationsGroupsSchemas(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockModel := model.NewMockModelInterface(ctrl)
	clusterID := uuid.New()
	mockModel.EXPECT().GetCluster(gomock.Any(), clusterID).Return(&querier.Cluster{
		ClusterUuid:         clusterID,
		ClusterName:         "dev",
		SqlConnectionString: "postgres://localhost:4566/dev",
		MetaNodeGrpcUrl:     "",
		MetaNodeHttpUrl:     "",
	}, nil)

	svc := NewService(mockModel, stubRisingWaveClient{
		listRelations: func(ctx context.Context, cfg risingwave.ClusterConfig, database string) ([]risingwave.RelationCategory, error) {
			return []risingwave.RelationCategory{
				{
					Name: "table",
					Schemas: []risingwave.RelationSchema{
						{
							SchemaName: "public",
							Relations: []risingwave.Relation{{
								SchemaName:   "public",
								RelationName: "users",
								RelationType: "table",
								Columns:      []risingwave.RelationColumn{{Name: "id", DataType: "integer"}},
							}},
						},
					},
				},
			}, nil
		},
	})

	result, err := svc.ListClusterRelations(context.Background(), clusterID, "dev")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(result.Categories) != 1 {
		t.Fatalf("expected 1 category, got %d", len(result.Categories))
	}
	if len(result.Categories[0].Schemas) != 1 {
		t.Fatalf("expected 1 schema, got %d", len(result.Categories[0].Schemas))
	}
	if result.Categories[0].Schemas[0].SchemaName != "public" {
		t.Fatalf("unexpected schema name: %s", result.Categories[0].Schemas[0].SchemaName)
	}
	if len(result.Categories[0].Schemas[0].Relations) != 1 {
		t.Fatalf("expected 1 relation, got %d", len(result.Categories[0].Schemas[0].Relations))
	}
	if result.Categories[0].Schemas[0].Relations[0].RelationName != "users" {
		t.Fatalf("unexpected relation name: %s", result.Categories[0].Schemas[0].Relations[0].RelationName)
	}
}

func TestInjectSelectLimit(t *testing.T) {
	statement := injectSelectLimit("select * from users")
	if statement != "SELECT * FROM users LIMIT 100" {
		t.Fatalf("unexpected rewritten statement: %q", statement)
	}
}

func TestInjectSelectLimitLeavesExistingLimit(t *testing.T) {
	statement := injectSelectLimit("select * from users limit 25")
	if statement != "select * from users limit 25" {
		t.Fatalf("expected existing limit to remain unchanged, got %q", statement)
	}
}

func TestExecuteClusterSQLInjectsLimitForSelect(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockModel := model.NewMockModelInterface(ctrl)
	clusterID := uuid.New()
	mockModel.EXPECT().GetCluster(gomock.Any(), clusterID).Return(&querier.Cluster{
		ClusterUuid:         clusterID,
		ClusterName:         "dev",
		SqlConnectionString: "postgres://localhost:4566/dev",
		MetaNodeGrpcUrl:     "",
		MetaNodeHttpUrl:     "",
	}, nil)

	svc := NewService(mockModel, stubRisingWaveClient{
		executeSQL: func(ctx context.Context, cfg risingwave.ClusterConfig, database, statement string) (*risingwave.SQLResult, error) {
			if statement != "SELECT * FROM users LIMIT 100" {
				t.Fatalf("unexpected statement: %q", statement)
			}
			return &risingwave.SQLResult{}, nil
		},
	})

	_, err := svc.ExecuteClusterSQL(context.Background(), clusterID, "dev", apigen.ExecuteSqlRequest{Statement: "select * from users"})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestExecuteClusterSQLReturnsEmptyArraysForCommandWithoutRows(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockModel := model.NewMockModelInterface(ctrl)
	clusterID := uuid.New()
	mockModel.EXPECT().GetCluster(gomock.Any(), clusterID).Return(&querier.Cluster{
		ClusterUuid:         clusterID,
		ClusterName:         "dev",
		SqlConnectionString: "postgres://localhost:4566/dev",
		MetaNodeGrpcUrl:     "",
		MetaNodeHttpUrl:     "",
	}, nil)

	svc := NewService(mockModel, stubRisingWaveClient{
		executeSQL: func(ctx context.Context, cfg risingwave.ClusterConfig, database, statement string) (*risingwave.SQLResult, error) {
			return &risingwave.SQLResult{
				Columns:      nil,
				Rows:         nil,
				CommandTag:   "CREATE TABLE",
				RowsAffected: 0,
			}, nil
		},
	})

	result, err := svc.ExecuteClusterSQL(context.Background(), clusterID, "dev", apigen.ExecuteSqlRequest{Statement: "create table t (id int)"})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if result.Columns == nil {
		t.Fatal("expected empty columns slice, got nil")
	}
	if result.Rows == nil {
		t.Fatal("expected empty rows slice, got nil")
	}
	if len(result.Columns) != 0 || len(result.Rows) != 0 {
		t.Fatalf("expected empty result slices, got %#v", result)
	}
	if result.CommandTag != "CREATE TABLE" {
		t.Fatalf("unexpected command tag: %s", result.CommandTag)
	}
}

func TestExecuteClusterSQLReturnsExecutionErrorInResponse(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockModel := model.NewMockModelInterface(ctrl)
	clusterID := uuid.New()
	mockModel.EXPECT().GetCluster(gomock.Any(), clusterID).Return(&querier.Cluster{
		ClusterUuid:         clusterID,
		ClusterName:         "dev",
		SqlConnectionString: "postgres://localhost:4566/dev",
		MetaNodeGrpcUrl:     "",
		MetaNodeHttpUrl:     "",
	}, nil)

	svc := NewService(mockModel, stubRisingWaveClient{
		executeSQL: func(ctx context.Context, cfg risingwave.ClusterConfig, database, statement string) (*risingwave.SQLResult, error) {
			return nil, fmt.Errorf("syntax error at or near \"from\"")
		},
	})

	result, err := svc.ExecuteClusterSQL(context.Background(), clusterID, "dev", apigen.ExecuteSqlRequest{Statement: "select from"})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if result.Error == nil || *result.Error != "syntax error at or near \"from\"" {
		t.Fatalf("expected SQL error in response, got %#v", result.Error)
	}
	if len(result.Columns) != 0 || len(result.Rows) != 0 || result.CommandTag != "" || result.RowsAffected != 0 {
		t.Fatalf("expected empty result payload on SQL error, got %#v", result)
	}
}

func TestReorderNotebookCellsUpdatesOrderInTransaction(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockModel := model.NewMockModelInterfaceWithTransaction(ctrl)
	notebookID := uuid.New()
	cellOne := uuid.New()
	cellTwo := uuid.New()

	mockModel.EXPECT().GetNotebook(gomock.Any(), notebookID).Return(&querier.Notebook{NotebookUuid: notebookID, NotebookName: "demo"}, nil)
	mockModel.EXPECT().ListNotebookCells(gomock.Any(), notebookID).Return([]*querier.NotebookCell{
		{CellUuid: cellOne, NotebookUuid: notebookID, OrderIndex: 0},
		{CellUuid: cellTwo, NotebookUuid: notebookID, OrderIndex: 1},
	}, nil)
	mockModel.EXPECT().UpdateNotebookCellOrder(gomock.Any(), querier.UpdateNotebookCellOrderParams{NotebookUuid: notebookID, CellUuid: cellTwo, OrderIndex: 2}).Return(nil)
	mockModel.EXPECT().UpdateNotebookCellOrder(gomock.Any(), querier.UpdateNotebookCellOrderParams{NotebookUuid: notebookID, CellUuid: cellOne, OrderIndex: 3}).Return(nil)
	mockModel.EXPECT().UpdateNotebookCellOrder(gomock.Any(), querier.UpdateNotebookCellOrderParams{NotebookUuid: notebookID, CellUuid: cellTwo, OrderIndex: 0}).Return(nil)
	mockModel.EXPECT().UpdateNotebookCellOrder(gomock.Any(), querier.UpdateNotebookCellOrderParams{NotebookUuid: notebookID, CellUuid: cellOne, OrderIndex: 1}).Return(nil)
	mockModel.EXPECT().UpdateNotebookTimestamp(gomock.Any(), notebookID).Return(nil)

	svc := NewService(mockModel, stubRisingWaveClient{})
	err := svc.ReorderNotebookCells(context.Background(), notebookID, apigen.ReorderNotebookCellsRequest{CellUuids: []uuid.UUID{cellTwo, cellOne}})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestUpdateNotebookCellPersistsBackgroundDDL(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockModel := model.NewMockModelInterface(ctrl)
	notebookID := uuid.New()
	cellID := uuid.New()
	backgroundDDL := true
	updatedAt := time.Now().UTC()

	mockModel.EXPECT().GetNotebook(gomock.Any(), notebookID).Return(&querier.Notebook{NotebookUuid: notebookID, NotebookName: "Notebook A"}, nil)
	mockModel.EXPECT().UpdateNotebookCell(gomock.Any(), querier.UpdateNotebookCellParams{
		NotebookUuid:  notebookID,
		CellUuid:      cellID,
		CellType:      string(apigen.NotebookCellTypeSQL),
		ClusterUuid:   uuid.NullUUID{},
		DatabaseName:  nil,
		BackgroundDdl: true,
		Content:       "select 1;",
	}).Return(&querier.NotebookCell{
		CellUuid:      cellID,
		NotebookUuid:  notebookID,
		CellType:      string(apigen.NotebookCellTypeSQL),
		BackgroundDdl: true,
		Content:       "select 1;",
		OrderIndex:    0,
		UpdatedAt:     updatedAt,
	}, nil)
	mockModel.EXPECT().UpdateNotebookTimestamp(gomock.Any(), notebookID).Return(nil)

	svc := NewService(mockModel, stubRisingWaveClient{})
	cell, err := svc.UpdateNotebookCell(context.Background(), notebookID, cellID, apigen.UpdateNotebookCellRequest{
		CellType:      apigen.NotebookCellTypeSQL,
		BackgroundDdl: &backgroundDDL,
		Content:       "select 1;",
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !cell.BackgroundDdl {
		t.Fatal("expected background DDL to be persisted")
	}
}

func TestGetNotebookMapsCells(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockModel := model.NewMockModelInterface(ctrl)
	notebookID := uuid.New()
	clusterID := uuid.New()
	database := "dev"
	updatedAt := time.Now().UTC()

	mockModel.EXPECT().GetNotebook(gomock.Any(), notebookID).Return(&querier.Notebook{
		NotebookUuid: notebookID,
		NotebookName: "Notebook A",
		UpdatedAt:    updatedAt,
	}, nil)
	mockModel.EXPECT().ListNotebookCells(gomock.Any(), notebookID).Return([]*querier.NotebookCell{
		{
			CellUuid:      uuid.New(),
			NotebookUuid:  notebookID,
			CellType:      string(apigen.NotebookCellTypeSQL),
			ClusterUuid:   uuid.NullUUID{UUID: clusterID, Valid: true},
			DatabaseName:  &database,
			BackgroundDdl: true,
			Content:       "select 1;",
			OrderIndex:    0,
			UpdatedAt:     updatedAt,
		},
	}, nil)

	svc := NewService(mockModel, stubRisingWaveClient{})
	notebook, err := svc.GetNotebook(context.Background(), notebookID)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if notebook.NotebookName != "Notebook A" {
		t.Fatalf("unexpected notebook name: %s", notebook.NotebookName)
	}
	if len(notebook.Cells) != 1 {
		t.Fatalf("expected 1 cell, got %d", len(notebook.Cells))
	}
	if notebook.Cells[0].ClusterUuid == nil || *notebook.Cells[0].ClusterUuid != clusterID {
		t.Fatalf("expected cluster uuid to be mapped")
	}
	if notebook.Cells[0].Database == nil || *notebook.Cells[0].Database != database {
		t.Fatalf("expected database to be mapped")
	}
	if !notebook.Cells[0].BackgroundDdl {
		t.Fatal("expected background DDL to be mapped")
	}
}
