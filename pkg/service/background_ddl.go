package service

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/cloudcarver/anclax/pkg/taskcore/store"
	"github.com/cloudcarver/waitkit/pkg/backgroundddl"
	"github.com/cloudcarver/waitkit/pkg/model"
	"github.com/cloudcarver/waitkit/pkg/risingwave"
	"github.com/cloudcarver/waitkit/pkg/zgen/apigen"
	"github.com/cloudcarver/waitkit/pkg/zgen/querier"
	backgroundddlparams "github.com/cloudcarver/waitkit/pkg/zgen/schemas/background_ddl"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func (s *Service) CreateBackgroundDDL(ctx context.Context, req apigen.CreateBackgroundDdlRequest) (*apigen.CreateBackgroundDdlResult, error) {
	if s.taskRunner == nil {
		return nil, fmt.Errorf("background DDL task runner is not configured")
	}
	if strings.TrimSpace(req.Database) == "" || strings.TrimSpace(req.Statement) == "" {
		return nil, fmt.Errorf("%w: clusterUuid, database, and statement are required", ErrInvalidInput)
	}
	if _, err := s.getCluster(ctx, req.ClusterUuid); err != nil {
		return nil, err
	}

	plans, err := backgroundddl.PlanStatements(req.Statement)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrInvalidInput, err.Error())
	}

	jobID := uuid.New()
	err = s.model.RunTransactionWithTx(ctx, func(tx pgx.Tx, txModel model.ModelInterface) error {
		if _, err := txModel.CreateBackgroundDdlJob(ctx, querier.CreateBackgroundDdlJobParams{
			ID:           jobID,
			ClusterUuid:  req.ClusterUuid,
			DatabaseName: req.Database,
			Statement:    req.Statement,
		}); err != nil {
			return err
		}

		for index, plan := range plans {
			if err := txModel.CreateBackgroundDdlProgress(ctx, querier.CreateBackgroundDdlProgressParams{
				ID:                   uuid.New(),
				JobID:                jobID,
				Seq:                  int32(index),
				Statement:            plan.Statement,
				StatementKind:        string(plan.Kind),
				TargetKind:           string(plan.TargetKind),
				TargetType:           nilIfEmpty(plan.TargetType),
				TargetSchema:         nilIfEmpty(plan.TargetSchema),
				TargetName:           nilIfEmpty(plan.TargetName),
				TargetIdentity:       nilIfEmpty(plan.TargetIdentity),
				ExpectRelationExists: plan.ExpectRelationExists,
			}); err != nil {
				return err
			}
		}

		taskID, err := s.taskRunner.RunBackgroundDDLWatcherWithTx(ctx, tx, &backgroundddlparams.BackgroundDDLWatcherParameters{
			BackgroundDdlJobId: jobID,
		}, store.WithUniqueTag(backgroundDDLTaskUniqueTag(jobID)))
		if err != nil {
			return err
		}
		return txModel.UpdateBackgroundDdlJobTaskID(ctx, querier.UpdateBackgroundDdlJobTaskIDParams{
			ID:     jobID,
			TaskID: &taskID,
		})
	})
	if err != nil {
		return nil, err
	}

	return &apigen.CreateBackgroundDdlResult{
		Id:     jobID,
		Status: apigen.BackgroundDdlStatusPending,
	}, nil
}

func (s *Service) ListBackgroundDDLs(ctx context.Context) (*apigen.BackgroundDdlList, error) {
	jobs, err := s.model.ListBackgroundDdlJobs(ctx)
	if err != nil {
		return nil, err
	}
	progresses, err := s.model.ListBackgroundDdlProgresses(ctx)
	if err != nil {
		return nil, err
	}

	progressesByJob := map[uuid.UUID][]*querier.BackgroundDdlProgress{}
	for _, progress := range progresses {
		progressesByJob[progress.JobID] = append(progressesByJob[progress.JobID], progress)
	}

	items := make([]apigen.BackgroundDdlJob, 0, len(jobs))
	for _, job := range jobs {
		jobProgresses := progressesByJob[job.ID]
		apiProgresses := make([]apigen.BackgroundDdlProgress, 0, len(jobProgresses))
		for _, progress := range jobProgresses {
			apiProgresses = append(apiProgresses, backgroundDdlProgressToAPI(progress))
		}
		item := apigen.BackgroundDdlJob{
			Id:          job.ID,
			ClusterUuid: job.ClusterUuid,
			Database:    job.DatabaseName,
			Statement:   job.Statement,
			Status:      backgroundDdlJobStatusToAPI(job),
			CreatedAt:   job.CreatedAt,
			Progresses:  apiProgresses,
		}
		if job.StartedAt != nil {
			item.StartedAt = job.StartedAt
		}
		if job.CancelRequestedAt != nil {
			item.CancelRequestedAt = job.CancelRequestedAt
		}
		if job.CancelledAt != nil {
			item.CancelledAt = job.CancelledAt
		}
		if job.FinishedAt != nil {
			item.FinishedAt = job.FinishedAt
		}
		if job.FailedAt != nil {
			item.FailedAt = job.FailedAt
		}
		if job.FailureReason != nil {
			item.FailureReason = job.FailureReason
		}
		items = append(items, item)
	}

	return &apigen.BackgroundDdlList{Jobs: items}, nil
}

func (s *Service) DeleteBackgroundDDL(ctx context.Context, id uuid.UUID) error {
	job, err := s.getBackgroundDdlJob(ctx, id)
	if err != nil {
		return err
	}
	if backgroundDdlJobIsTerminal(job) {
		return nil
	}

	progresses, err := s.model.ListBackgroundDdlProgressesByJob(ctx, id)
	if err != nil {
		return err
	}

	var active *querier.BackgroundDdlProgress
	for _, progress := range progresses {
		if backgroundDdlProgressIsTerminal(progress) {
			continue
		}
		if progress.StartedAt != nil {
			active = progress
			break
		}
	}

	if err := s.model.RunTransaction(ctx, func(txModel model.ModelInterface) error {
		if err := txModel.MarkBackgroundDdlJobCancelRequested(ctx, id); err != nil {
			return err
		}
		if err := txModel.CancelPendingBackgroundDdlProgresses(ctx, id); err != nil {
			return err
		}
		if active == nil {
			return txModel.MarkBackgroundDdlJobCancelled(ctx, id)
		}
		return nil
	}); err != nil {
		return err
	}

	if active == nil {
		return nil
	}

	cluster, err := s.getCluster(ctx, job.ClusterUuid)
	if err != nil {
		return nil
	}
	jobIDs := append([]int64(nil), active.RwJobIds...)
	if len(jobIDs) == 0 && active.StatementKind == string(backgroundddl.StatementKindTrackedDDL) {
		if backgroundJobs, lookupErr := s.risingwave.ListBackgroundJobsByStatement(ctx, clusterToRWConfig(cluster), job.DatabaseName, active.Statement); lookupErr == nil {
			jobIDs = uniqueBackgroundJobIDs(backgroundJobs)
		}
	}
	if len(jobIDs) == 0 {
		return nil
	}
	_ = s.risingwave.CancelJobs(ctx, clusterToRWConfig(cluster), job.DatabaseName, jobIDs)
	return nil
}

func (s *Service) getBackgroundDdlJob(ctx context.Context, id uuid.UUID) (*querier.BackgroundDdlJob, error) {
	job, err := s.model.GetBackgroundDdlJob(ctx, id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrBackgroundDdlNotFound
		}
		return nil, err
	}
	return job, nil
}

func backgroundDDLTaskUniqueTag(jobID uuid.UUID) string {
	return "background-ddl-job:" + jobID.String()
}

func nilIfEmpty(value string) *string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil
	}
	return &trimmed
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

func backgroundDdlJobIsTerminal(job *querier.BackgroundDdlJob) bool {
	return job.CancelledAt != nil || job.FinishedAt != nil || job.FailedAt != nil
}

func backgroundDdlProgressIsTerminal(progress *querier.BackgroundDdlProgress) bool {
	return progress.CancelledAt != nil || progress.FinishedAt != nil || progress.FailedAt != nil
}

func backgroundDdlJobStatusToAPI(job *querier.BackgroundDdlJob) apigen.BackgroundDdlStatus {
	switch {
	case job.FailedAt != nil:
		return apigen.BackgroundDdlStatusFailed
	case job.CancelledAt != nil:
		return apigen.BackgroundDdlStatusCancelled
	case job.FinishedAt != nil:
		return apigen.BackgroundDdlStatusFinished
	case job.CancelRequestedAt != nil:
		return apigen.BackgroundDdlStatusCancelRequested
	case job.StartedAt != nil:
		return apigen.BackgroundDdlStatusRunning
	default:
		return apigen.BackgroundDdlStatusPending
	}
}

func backgroundDdlProgressToAPI(progress *querier.BackgroundDdlProgress) apigen.BackgroundDdlProgress {
	item := apigen.BackgroundDdlProgress{
		Id:            progress.ID,
		Seq:           progress.Seq,
		Statement:     progress.Statement,
		StatementKind: apigen.BackgroundDdlStatementKind(progress.StatementKind),
		Status:        backgroundDdlProgressStatusToAPI(progress),
		TargetKind:    apigen.BackgroundDdlTargetKind(progress.TargetKind),
		RwJobIds:      append([]int64(nil), progress.RwJobIds...),
	}
	if item.RwJobIds == nil {
		item.RwJobIds = []int64{}
	}
	if progress.TargetType != nil {
		item.TargetType = progress.TargetType
	}
	if progress.TargetSchema != nil {
		item.TargetSchema = progress.TargetSchema
	}
	if progress.TargetName != nil {
		item.TargetName = progress.TargetName
	}
	if progress.TargetIdentity != nil {
		item.TargetIdentity = progress.TargetIdentity
	}
	if progress.LastProgress != nil {
		item.LastProgress = progress.LastProgress
	}
	if progress.LastProgressTrackedAt != nil {
		item.LastProgressTrackedAt = progress.LastProgressTrackedAt
	}
	if progress.EstimatedFinishedAt != nil {
		item.EstimatedFinishedAt = progress.EstimatedFinishedAt
	}
	if progress.StartedAt != nil {
		item.StartedAt = progress.StartedAt
	}
	if progress.FinishedAt != nil {
		item.FinishedAt = progress.FinishedAt
	}
	if progress.CancelledAt != nil {
		item.CancelledAt = progress.CancelledAt
	}
	if progress.FailedAt != nil {
		item.FailedAt = progress.FailedAt
	}
	if progress.FailureReason != nil {
		item.FailureReason = progress.FailureReason
	}
	return item
}

func backgroundDdlProgressStatusToAPI(progress *querier.BackgroundDdlProgress) apigen.BackgroundDdlStatus {
	switch {
	case progress.FailedAt != nil:
		return apigen.BackgroundDdlStatusFailed
	case progress.CancelledAt != nil:
		return apigen.BackgroundDdlStatusCancelled
	case progress.FinishedAt != nil:
		return apigen.BackgroundDdlStatusFinished
	case progress.StartedAt != nil:
		return apigen.BackgroundDdlStatusRunning
	default:
		return apigen.BackgroundDdlStatusPending
	}
}
