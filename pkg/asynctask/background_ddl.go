package asynctask

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/cloudcarver/anclax/core"
	taskstore "github.com/cloudcarver/anclax/pkg/taskcore/store"
	"github.com/cloudcarver/anclax/pkg/taskcore/worker"
	"github.com/cloudcarver/waitkit/pkg/backgroundddl"
	"github.com/cloudcarver/waitkit/pkg/model"
	"github.com/cloudcarver/waitkit/pkg/risingwave"
	"github.com/cloudcarver/waitkit/pkg/zgen/querier"
	backgroundddlparams "github.com/cloudcarver/waitkit/pkg/zgen/schemas/background_ddl"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const backgroundDDLMissingProgressTimeout = 30 * time.Second

func (e *Executor) ExecuteBackgroundDDLWatcher(ctx context.Context, _ worker.Task, params *backgroundddlparams.BackgroundDDLWatcherParameters) error {
	if params == nil || params.BackgroundDdlJobId == uuid.Nil {
		return fmt.Errorf("%w: background DDL job id is required", taskstore.ErrFatalTask)
	}

	job, err := e.getBackgroundDdlJob(ctx, params.BackgroundDdlJobId)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("%w: background DDL job not found", taskstore.ErrFatalTask)
		}
		return err
	}
	if backgroundDdlJobIsTerminal(job) {
		return nil
	}

	progresses, err := e.model.ListBackgroundDdlProgressesByJob(ctx, job.ID)
	if err != nil {
		return err
	}
	if len(progresses) == 0 {
		return e.failJob(ctx, job.ID, "background DDL job has no statements")
	}
	if job.CancelRequestedAt != nil {
		if err := e.model.CancelPendingBackgroundDdlProgresses(ctx, job.ID); err != nil {
			return err
		}
	}

	current := nextBackgroundDdlProgress(progresses)
	if current == nil {
		return e.finalizeJob(ctx, job.ID)
	}

	cluster, err := e.model.GetCluster(ctx, job.ClusterUuid)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return e.failProgressAndJob(ctx, job.ID, current.ID, "cluster not found")
		}
		return err
	}

	preamble := buildPreamble(progresses, current.Seq)
	switch current.StatementKind {
	case string(backgroundddl.StatementKindSet), string(backgroundddl.StatementKindDirect):
		return e.executeDirectProgress(ctx, job, current, cluster, preamble)
	case string(backgroundddl.StatementKindTrackedDDL):
		return e.executeTrackedProgress(ctx, job, current, cluster, preamble)
	default:
		return e.failProgressAndJob(ctx, job.ID, current.ID, fmt.Sprintf("unsupported statement kind %q", current.StatementKind))
	}
}

func (e *Executor) OnBackgroundDDLWatcherFailed(ctx context.Context, taskID int32, params *backgroundddlparams.BackgroundDDLWatcherParameters, tx core.Tx) error {
	_ = taskID
	if params == nil || params.BackgroundDdlJobId == uuid.Nil {
		return nil
	}
	pgxTx, ok := tx.(pgx.Tx)
	if !ok {
		return nil
	}
	txModel := e.model.SpawnWithTx(pgxTx)
	job, err := txModel.GetBackgroundDdlJob(ctx, params.BackgroundDdlJobId)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		return err
	}
	if backgroundDdlJobIsTerminal(job) {
		return nil
	}
	if err := txModel.MarkBackgroundDdlJobFailed(ctx, querier.MarkBackgroundDdlJobFailedParams{
		ID:            job.ID,
		FailureReason: ptr("background DDL watcher task failed"),
	}); err != nil {
		return err
	}
	progress, err := txModel.GetNextBackgroundDdlProgress(ctx, job.ID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		return err
	}
	if backgroundDdlProgressIsTerminal(progress) {
		return nil
	}
	return txModel.MarkBackgroundDdlProgressFailed(ctx, querier.MarkBackgroundDdlProgressFailedParams{
		ID:            progress.ID,
		FailureReason: ptr("background DDL watcher task failed"),
	})
}

func (e *Executor) executeDirectProgress(ctx context.Context, job *querier.BackgroundDdlJob, progress *querier.BackgroundDdlProgress, cluster *querier.Cluster, preamble []string) error {
	if progress.DispatchedAt != nil {
		return e.failProgressAndJob(ctx, job.ID, progress.ID, "statement execution outcome is unknown after task retry")
	}
	if err := e.markStarted(ctx, job.ID, progress.ID); err != nil {
		return err
	}
	statement := backgroundddl.ComposeStatements(append(preamble, progress.Statement)...)
	if _, err := e.risingwave.ExecuteSQL(ctx, clusterToRWConfig(cluster), job.DatabaseName, statement); err != nil {
		return e.failProgressAndJob(ctx, job.ID, progress.ID, err.Error())
	}
	if err := e.model.MarkBackgroundDdlProgressFinished(ctx, progress.ID); err != nil {
		return err
	}
	return e.advanceOrFinalize(ctx, job.ID)
}

func (e *Executor) executeTrackedProgress(ctx context.Context, job *querier.BackgroundDdlJob, progress *querier.BackgroundDdlProgress, cluster *querier.Cluster, preamble []string) error {
	completed, err := e.trackedProgressCompleted(ctx, job, progress, cluster)
	if err != nil {
		return err
	}
	if completed {
		if err := e.model.MarkBackgroundDdlProgressFinished(ctx, progress.ID); err != nil {
			return err
		}
		return e.advanceOrFinalize(ctx, job.ID)
	}

	if progress.DispatchedAt == nil {
		if err := e.markStarted(ctx, job.ID, progress.ID); err != nil {
			return err
		}
		statementParts := append(append([]string{}, preamble...), "SET BACKGROUND_DDL=true", progress.Statement)
		statement := backgroundddl.ComposeStatements(statementParts...)
		if _, err := e.risingwave.ExecuteSQL(ctx, clusterToRWConfig(cluster), job.DatabaseName, statement); err != nil {
			return e.failProgressAndJob(ctx, job.ID, progress.ID, err.Error())
		}
		return taskstore.ErrRetryTaskWithoutErrorEvent
	}

	backgroundJobs, err := e.risingwave.ListBackgroundJobsByStatement(ctx, clusterToRWConfig(cluster), job.DatabaseName, progress.Statement)
	if err != nil {
		return err
	}
	if len(backgroundJobs) > 0 {
		jobIDs := uniqueBackgroundJobIDs(backgroundJobs)
		lastProgress := highestBackgroundJobProgress(backgroundJobs)
		now := time.Now().UTC()
		estimatedFinishedAt := estimateFinishedAt(progress.LastProgress, progress.LastProgressTrackedAt, lastProgress, now)
		var trackedAt *time.Time
		if lastProgress != nil {
			trackedAt = &now
		}
		if err := e.model.UpdateBackgroundDdlProgressTracking(ctx, querier.UpdateBackgroundDdlProgressTrackingParams{
			ID:                    progress.ID,
			RwJobIds:              jobIDs,
			LastProgress:          lastProgress,
			LastProgressTrackedAt: trackedAt,
			EstimatedFinishedAt:   estimatedFinishedAt,
		}); err != nil {
			return err
		}
		if job.CancelRequestedAt != nil {
			_ = e.risingwave.CancelJobs(ctx, clusterToRWConfig(cluster), job.DatabaseName, jobIDs)
		}
		return taskstore.ErrRetryTaskWithoutErrorEvent
	}

	if job.CancelRequestedAt != nil {
		if err := e.model.MarkBackgroundDdlProgressCancelled(ctx, progress.ID); err != nil {
			return err
		}
		return e.advanceOrFinalize(ctx, job.ID)
	}
	if progress.DispatchedAt != nil && time.Since(*progress.DispatchedAt) > backgroundDDLMissingProgressTimeout {
		return e.failProgressAndJob(ctx, job.ID, progress.ID, "background DDL progress not found in RisingWave")
	}
	return taskstore.ErrRetryTaskWithoutErrorEvent
}

func (e *Executor) trackedProgressCompleted(ctx context.Context, job *querier.BackgroundDdlJob, progress *querier.BackgroundDdlProgress, cluster *querier.Cluster) (bool, error) {
	if progress.TargetKind != string(backgroundddl.TargetKindRelation) || progress.TargetName == nil {
		return false, nil
	}
	schema := "public"
	if progress.TargetSchema != nil && strings.TrimSpace(*progress.TargetSchema) != "" {
		schema = *progress.TargetSchema
	}
	relationType := ""
	if progress.TargetType != nil {
		relationType = *progress.TargetType
	}
	relation, err := e.risingwave.FindRelation(ctx, clusterToRWConfig(cluster), job.DatabaseName, schema, *progress.TargetName, relationType)
	if err != nil {
		return false, err
	}
	expectExists := progress.ExpectRelationExists != nil && *progress.ExpectRelationExists
	if expectExists {
		return relation != nil, nil
	}
	return relation == nil, nil
}

func (e *Executor) markStarted(ctx context.Context, jobID uuid.UUID, progressID uuid.UUID) error {
	return e.model.RunTransaction(ctx, func(txModel model.ModelInterface) error {
		if err := txModel.MarkBackgroundDdlJobStarted(ctx, jobID); err != nil {
			return err
		}
		return txModel.MarkBackgroundDdlProgressStarted(ctx, progressID)
	})
}

func (e *Executor) advanceOrFinalize(ctx context.Context, jobID uuid.UUID) error {
	progresses, err := e.model.ListBackgroundDdlProgressesByJob(ctx, jobID)
	if err != nil {
		return err
	}
	if nextBackgroundDdlProgress(progresses) != nil {
		return taskstore.ErrRetryTaskWithoutErrorEvent
	}
	return e.finalizeJob(ctx, jobID)
}

func (e *Executor) finalizeJob(ctx context.Context, jobID uuid.UUID) error {
	progresses, err := e.model.ListBackgroundDdlProgressesByJob(ctx, jobID)
	if err != nil {
		return err
	}
	cancelled := false
	for _, progress := range progresses {
		if progress.CancelledAt != nil {
			cancelled = true
			break
		}
	}
	if cancelled {
		return e.model.MarkBackgroundDdlJobCancelled(ctx, jobID)
	}
	return e.model.MarkBackgroundDdlJobFinished(ctx, jobID)
}

func (e *Executor) failProgressAndJob(ctx context.Context, jobID uuid.UUID, progressID uuid.UUID, reason string) error {
	if err := e.model.RunTransaction(ctx, func(txModel model.ModelInterface) error {
		if err := txModel.MarkBackgroundDdlProgressFailed(ctx, querier.MarkBackgroundDdlProgressFailedParams{
			ID:            progressID,
			FailureReason: ptr(reason),
		}); err != nil {
			return err
		}
		return txModel.MarkBackgroundDdlJobFailed(ctx, querier.MarkBackgroundDdlJobFailedParams{
			ID:            jobID,
			FailureReason: ptr(reason),
		})
	}); err != nil {
		return err
	}
	return fmt.Errorf("%w: %s", taskstore.ErrFatalTask, reason)
}

func (e *Executor) failJob(ctx context.Context, jobID uuid.UUID, reason string) error {
	if err := e.model.MarkBackgroundDdlJobFailed(ctx, querier.MarkBackgroundDdlJobFailedParams{
		ID:            jobID,
		FailureReason: ptr(reason),
	}); err != nil {
		return err
	}
	return fmt.Errorf("%w: %s", taskstore.ErrFatalTask, reason)
}

func (e *Executor) getBackgroundDdlJob(ctx context.Context, id uuid.UUID) (*querier.BackgroundDdlJob, error) {
	return e.model.GetBackgroundDdlJob(ctx, id)
}

func nextBackgroundDdlProgress(progresses []*querier.BackgroundDdlProgress) *querier.BackgroundDdlProgress {
	for _, progress := range progresses {
		if !backgroundDdlProgressIsTerminal(progress) {
			return progress
		}
	}
	return nil
}

func buildPreamble(progresses []*querier.BackgroundDdlProgress, untilSeq int32) []string {
	preamble := make([]string, 0)
	for _, progress := range progresses {
		if progress.Seq >= untilSeq {
			break
		}
		if progress.StatementKind != string(backgroundddl.StatementKindSet) || progress.FinishedAt == nil {
			continue
		}
		preamble = append(preamble, progress.Statement)
	}
	return preamble
}

func uniqueBackgroundJobIDs(jobs []risingwave.BackgroundJobProgress) []int64 {
	seen := map[int64]struct{}{}
	ids := make([]int64, 0, len(jobs))
	for _, job := range jobs {
		if job.JobID == 0 {
			continue
		}
		if _, ok := seen[job.JobID]; ok {
			continue
		}
		seen[job.JobID] = struct{}{}
		ids = append(ids, job.JobID)
	}
	return ids
}

func highestBackgroundJobProgress(jobs []risingwave.BackgroundJobProgress) *float64 {
	var best *float64
	for _, job := range jobs {
		if job.Progress == nil {
			continue
		}
		if best == nil || *job.Progress > *best {
			value := *job.Progress
			best = &value
		}
	}
	return best
}

func estimateFinishedAt(lastProgress *float64, lastTrackedAt *time.Time, currentProgress *float64, now time.Time) *time.Time {
	if lastProgress == nil || lastTrackedAt == nil || currentProgress == nil {
		return nil
	}
	if *currentProgress <= *lastProgress {
		return nil
	}
	elapsed := now.Sub(*lastTrackedAt)
	if elapsed <= 0 {
		return nil
	}
	remaining := 100 - *currentProgress
	if remaining <= 0 {
		finishedAt := now
		return &finishedAt
	}
	rate := (*currentProgress - *lastProgress) / elapsed.Seconds()
	if rate <= 0 {
		return nil
	}
	estimated := now.Add(time.Duration((remaining / rate) * float64(time.Second)))
	return &estimated
}

func backgroundDdlJobIsTerminal(job *querier.BackgroundDdlJob) bool {
	return job.CancelledAt != nil || job.FinishedAt != nil || job.FailedAt != nil
}

func backgroundDdlProgressIsTerminal(progress *querier.BackgroundDdlProgress) bool {
	return progress.CancelledAt != nil || progress.FinishedAt != nil || progress.FailedAt != nil
}

func clusterToRWConfig(cluster *querier.Cluster) risingwave.ClusterConfig {
	return risingwave.ClusterConfig{
		SQLConnectionString: cluster.SqlConnectionString,
		MetaNodeGrpcURL:     cluster.MetaNodeGrpcUrl,
		MetaNodeHTTPURL:     cluster.MetaNodeHttpUrl,
	}
}

func ptr(value string) *string {
	return &value
}
