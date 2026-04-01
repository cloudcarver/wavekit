/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { TaskCronjob } from './TaskCronjob';
import type { TaskRetryPolicy } from './TaskRetryPolicy';
export type TaskAttributes = {
    /**
     * Strict priority of the task. Higher number runs first. Zero means normal weighted scheduling.
     */
    priority?: number;
    /**
     * Relative weight of the task within weighted scheduling (normal tasks only).
     */
    weight?: number;
    /**
     * Timeout of the task, a valid go time duration value e.g. 1h, 1d, 1w, 1m
     */
    timeout?: string;
    cronjob?: TaskCronjob;
    retryPolicy?: TaskRetryPolicy;
    labels?: Array<string>;
    /**
     * Serial key for forcing tasks with the same key to run one by one
     */
    serialKey?: string;
    /**
     * Optional serial order within the same serial key (lower runs first)
     */
    serialID?: number;
};

