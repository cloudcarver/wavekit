/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { BackgroundProgressDatabase } from './BackgroundProgressDatabase';
export type BackgroundProgressCluster = {
    clusterUuid: string;
    clusterName: string;
    databases: Array<BackgroundProgressDatabase>;
    error?: string;
};

