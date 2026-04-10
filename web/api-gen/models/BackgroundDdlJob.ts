/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { BackgroundDdlProgress } from './BackgroundDdlProgress';
import type { BackgroundDdlStatus } from './BackgroundDdlStatus';
export type BackgroundDdlJob = {
    id: string;
    clusterUuid: string;
    database: string;
    statement: string;
    status: BackgroundDdlStatus;
    createdAt: string;
    startedAt?: string | null;
    cancelRequestedAt?: string | null;
    cancelledAt?: string | null;
    finishedAt?: string | null;
    failedAt?: string | null;
    failureReason?: string | null;
    progresses: Array<BackgroundDdlProgress>;
};

