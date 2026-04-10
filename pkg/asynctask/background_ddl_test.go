package asynctask

import (
	"context"
	"testing"
	"time"

	"github.com/cloudcarver/waitkit/pkg/model"
	"github.com/cloudcarver/waitkit/pkg/risingwave"
	"github.com/cloudcarver/waitkit/pkg/zgen/querier"
	backgroundddlparams "github.com/cloudcarver/waitkit/pkg/zgen/schemas/background_ddl"
	"github.com/google/uuid"
	"go.uber.org/mock/gomock"

	taskworker "github.com/cloudcarver/anclax/pkg/taskcore/worker"
)

type stubRisingWaveClient struct {
	executeSQL                    func(ctx context.Context, cfg risingwave.ClusterConfig, database, statement string) (*risingwave.SQLResult, error)
	findRelation                  func(ctx context.Context, cfg risingwave.ClusterConfig, database, schema, relationName, relationType string) (*risingwave.Relation, error)
	listBackgroundJobsByStatement func(ctx context.Context, cfg risingwave.ClusterConfig, database, statement string) ([]risingwave.BackgroundJobProgress, error)
	cancelJobs                    func(ctx context.Context, cfg risingwave.ClusterConfig, database string, jobIDs []int64) error
}

func (s stubRisingWaveClient) ValidateCluster(ctx context.Context, cfg risingwave.ClusterConfig) (*risingwave.ConnectionStatus, error) {
	return &risingwave.ConnectionStatus{}, nil
}

func (s stubRisingWaveClient) ListDatabases(ctx context.Context, cfg risingwave.ClusterConfig) ([]string, error) {
	return nil, nil
}

func (s stubRisingWaveClient) ListRelations(ctx context.Context, cfg risingwave.ClusterConfig, database string) ([]risingwave.RelationCategory, error) {
	return nil, nil
}

func (s stubRisingWaveClient) ExecuteSQL(ctx context.Context, cfg risingwave.ClusterConfig, database, statement string) (*risingwave.SQLResult, error) {
	if s.executeSQL == nil {
		return &risingwave.SQLResult{}, nil
	}
	return s.executeSQL(ctx, cfg, database, statement)
}

func (s stubRisingWaveClient) FindRelation(ctx context.Context, cfg risingwave.ClusterConfig, database, schema, relationName, relationType string) (*risingwave.Relation, error) {
	if s.findRelation == nil {
		return nil, nil
	}
	return s.findRelation(ctx, cfg, database, schema, relationName, relationType)
}

func (s stubRisingWaveClient) ListBackgroundJobsByStatement(ctx context.Context, cfg risingwave.ClusterConfig, database, statement string) ([]risingwave.BackgroundJobProgress, error) {
	if s.listBackgroundJobsByStatement == nil {
		return nil, nil
	}
	return s.listBackgroundJobsByStatement(ctx, cfg, database, statement)
}

func (s stubRisingWaveClient) CancelJobs(ctx context.Context, cfg risingwave.ClusterConfig, database string, jobIDs []int64) error {
	if s.cancelJobs == nil {
		return nil
	}
	return s.cancelJobs(ctx, cfg, database, jobIDs)
}

func TestExecuteBackgroundDDLWatcherRunsDirectStatementAndFinishesJob(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockModel := model.NewMockModelInterfaceWithTransaction(ctrl)
	jobID := uuid.New()
	clusterID := uuid.New()
	progressID := uuid.New()
	createdAt := time.Now().UTC()
	startedAt := createdAt.Add(time.Second)
	finishedAt := startedAt.Add(time.Second)

	mockModel.EXPECT().GetBackgroundDdlJob(gomock.Any(), jobID).Return(&querier.BackgroundDdlJob{
		ID:           jobID,
		ClusterUuid:  clusterID,
		DatabaseName: "dev",
		Statement:    "CREATE FUNCTION touch_users() RETURNS void LANGUAGE SQL AS $$ SELECT 1 $$",
		CreatedAt:    createdAt,
	}, nil)
	mockModel.EXPECT().ListBackgroundDdlProgressesByJob(gomock.Any(), jobID).Return([]*querier.BackgroundDdlProgress{
		{ID: progressID, JobID: jobID, Seq: 0, Statement: "CREATE FUNCTION touch_users() RETURNS void LANGUAGE SQL AS $$ SELECT 1 $$", StatementKind: "DIRECT", TargetKind: "function"},
	}, nil)
	mockModel.EXPECT().GetCluster(gomock.Any(), clusterID).Return(&querier.Cluster{ClusterUuid: clusterID, ClusterName: "dev", SqlConnectionString: "postgres://localhost:4566/dev"}, nil)
	mockModel.EXPECT().MarkBackgroundDdlJobStarted(gomock.Any(), jobID).Return(nil)
	mockModel.EXPECT().MarkBackgroundDdlProgressStarted(gomock.Any(), progressID).Return(nil)
	mockModel.EXPECT().MarkBackgroundDdlProgressFinished(gomock.Any(), progressID).Return(nil)
	mockModel.EXPECT().ListBackgroundDdlProgressesByJob(gomock.Any(), jobID).Return([]*querier.BackgroundDdlProgress{
		{ID: progressID, JobID: jobID, Seq: 0, Statement: "CREATE FUNCTION touch_users() RETURNS void LANGUAGE SQL AS $$ SELECT 1 $$", StatementKind: "DIRECT", TargetKind: "function", StartedAt: &startedAt, FinishedAt: &finishedAt},
	}, nil)
	mockModel.EXPECT().ListBackgroundDdlProgressesByJob(gomock.Any(), jobID).Return([]*querier.BackgroundDdlProgress{
		{ID: progressID, JobID: jobID, Seq: 0, Statement: "CREATE FUNCTION touch_users() RETURNS void LANGUAGE SQL AS $$ SELECT 1 $$", StatementKind: "DIRECT", TargetKind: "function", StartedAt: &startedAt, FinishedAt: &finishedAt},
	}, nil)
	mockModel.EXPECT().MarkBackgroundDdlJobFinished(gomock.Any(), jobID).Return(nil)

	exec := &Executor{
		model: mockModel,
		risingwave: stubRisingWaveClient{
			executeSQL: func(ctx context.Context, cfg risingwave.ClusterConfig, database, statement string) (*risingwave.SQLResult, error) {
				if database != "dev" || statement != "CREATE FUNCTION touch_users() RETURNS void LANGUAGE SQL AS $$ SELECT 1 $$" {
					t.Fatalf("unexpected SQL execution: database=%s statement=%q", database, statement)
				}
				return &risingwave.SQLResult{CommandTag: "CREATE FUNCTION"}, nil
			},
		},
	}

	err := exec.ExecuteBackgroundDDLWatcher(context.Background(), taskworker.Task{}, &backgroundddlparams.BackgroundDDLWatcherParameters{BackgroundDdlJobId: jobID})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}
