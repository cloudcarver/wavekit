/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { BackgroundDdlStatementKind } from './BackgroundDdlStatementKind';
import type { BackgroundDdlStatus } from './BackgroundDdlStatus';
import type { BackgroundDdlTargetKind } from './BackgroundDdlTargetKind';
export type BackgroundDdlProgress = {
    id: string;
    seq: number;
    statement: string;
    statementKind: BackgroundDdlStatementKind;
    status: BackgroundDdlStatus;
    targetKind: BackgroundDdlTargetKind;
    targetType?: string | null;
    targetSchema?: string | null;
    targetName?: string | null;
    targetIdentity?: string | null;
    rwJobIds: Array<number>;
    lastProgress?: number | null;
    lastProgressTrackedAt?: string | null;
    estimatedFinishedAt?: string | null;
    startedAt?: string | null;
    finishedAt?: string | null;
    cancelledAt?: string | null;
    failedAt?: string | null;
    failureReason?: string | null;
};

