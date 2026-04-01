/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { ConnectionStatus } from './ConnectionStatus';
export type Cluster = {
    clusterUuid: string;
    clusterName: string;
    sqlConnectionString: string;
    metaNodeGrpcUrl: string;
    metaNodeHttpUrl: string;
    connectionStatus: ConnectionStatus;
};

