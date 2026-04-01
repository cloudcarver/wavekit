/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { TaskAttributes } from './TaskAttributes';
import type { TaskSpec } from './TaskSpec';
export type Task = {
    ID: number;
    /**
     * Parent task ID if this task was spawned from another task
     */
    parentTaskId?: number;
    attributes: TaskAttributes;
    spec: TaskSpec;
    status: Task.status;
    /**
     * Unique tag of the task
     */
    uniqueTag?: string;
    /**
     * Worker ID that currently owns the task
     */
    workerId?: string;
    startedAt?: string;
    lockedAt?: string;
    createdAt: string;
    updatedAt: string;
    events: Array<'onFailed'>;
    /**
     * The number of times the task has been attempted
     */
    attempts: number;
};
export namespace Task {
    export enum status {
        PENDING = 'pending',
        COMPLETED = 'completed',
        FAILED = 'failed',
        PAUSED = 'paused',
        CANCELLED = 'cancelled',
    }
}

