/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { BackgroundProgressList } from '../models/BackgroundProgressList';
import type { Cluster } from '../models/Cluster';
import type { ClusterDatabaseList } from '../models/ClusterDatabaseList';
import type { ClusterList } from '../models/ClusterList';
import type { ClusterRelationList } from '../models/ClusterRelationList';
import type { ConnectClusterRequest } from '../models/ConnectClusterRequest';
import type { ConnectClusterResult } from '../models/ConnectClusterResult';
import type { CreateNotebookCellRequest } from '../models/CreateNotebookCellRequest';
import type { CreateNotebookCellResult } from '../models/CreateNotebookCellResult';
import type { CreateNotebookRequest } from '../models/CreateNotebookRequest';
import type { CreateNotebookResult } from '../models/CreateNotebookResult';
import type { ExecuteSqlRequest } from '../models/ExecuteSqlRequest';
import type { Notebook } from '../models/Notebook';
import type { NotebookCell } from '../models/NotebookCell';
import type { NotebookList } from '../models/NotebookList';
import type { ReorderNotebookCellsRequest } from '../models/ReorderNotebookCellsRequest';
import type { SqlExecutionResult } from '../models/SqlExecutionResult';
import type { UpdateClusterRequest } from '../models/UpdateClusterRequest';
import type { UpdateNotebookCellRequest } from '../models/UpdateNotebookCellRequest';
import type { CancelablePromise } from '../core/CancelablePromise';
import { OpenAPI } from '../core/OpenAPI';
import { request as __request } from '../core/request';
export class DefaultService {
    /**
     * Connect a RisingWave cluster
     * @param requestBody
     * @returns ConnectClusterResult Connected cluster
     * @throws ApiError
     */
    public static connectCluster(
        requestBody: ConnectClusterRequest,
    ): CancelablePromise<ConnectClusterResult> {
        return __request(OpenAPI, {
            method: 'POST',
            url: '/clusters/connect',
            body: requestBody,
            mediaType: 'application/json',
        });
    }
    /**
     * List connected clusters
     * @returns ClusterList Connected clusters
     * @throws ApiError
     */
    public static listClusters(): CancelablePromise<ClusterList> {
        return __request(OpenAPI, {
            method: 'GET',
            url: '/clusters',
        });
    }
    /**
     * List background DDL and CDC progress across all clusters and databases
     * @returns BackgroundProgressList Background progress by cluster and database
     * @throws ApiError
     */
    public static listClusterBackgroundProgress(): CancelablePromise<BackgroundProgressList> {
        return __request(OpenAPI, {
            method: 'GET',
            url: '/clusters/background-progress',
        });
    }
    /**
     * Update cluster settings
     * @param clusterUuid
     * @param requestBody
     * @returns Cluster Updated cluster
     * @throws ApiError
     */
    public static updateCluster(
        clusterUuid: string,
        requestBody: UpdateClusterRequest,
    ): CancelablePromise<Cluster> {
        return __request(OpenAPI, {
            method: 'PUT',
            url: '/clusters/{cluster_uuid}',
            path: {
                'cluster_uuid': clusterUuid,
            },
            body: requestBody,
            mediaType: 'application/json',
        });
    }
    /**
     * Delete a cluster connection
     * @param clusterUuid
     * @returns void
     * @throws ApiError
     */
    public static deleteCluster(
        clusterUuid: string,
    ): CancelablePromise<void> {
        return __request(OpenAPI, {
            method: 'DELETE',
            url: '/clusters/{cluster_uuid}',
            path: {
                'cluster_uuid': clusterUuid,
            },
        });
    }
    /**
     * List databases for a cluster
     * @param clusterUuid
     * @returns ClusterDatabaseList Cluster databases
     * @throws ApiError
     */
    public static listClusterDatabases(
        clusterUuid: string,
    ): CancelablePromise<ClusterDatabaseList> {
        return __request(OpenAPI, {
            method: 'GET',
            url: '/clusters/{cluster_uuid}/databases',
            path: {
                'cluster_uuid': clusterUuid,
            },
        });
    }
    /**
     * List relation metadata for a cluster database
     * @param clusterUuid
     * @param database
     * @returns ClusterRelationList Relation explorer data
     * @throws ApiError
     */
    public static listClusterRelations(
        clusterUuid: string,
        database: string,
    ): CancelablePromise<ClusterRelationList> {
        return __request(OpenAPI, {
            method: 'GET',
            url: '/clusters/{cluster_uuid}/{database}/relations',
            path: {
                'cluster_uuid': clusterUuid,
                'database': database,
            },
        });
    }
    /**
     * Execute SQL against a cluster database
     * @param clusterUuid
     * @param database
     * @param requestBody
     * @returns SqlExecutionResult SQL execution result
     * @throws ApiError
     */
    public static executeClusterSql(
        clusterUuid: string,
        database: string,
        requestBody: ExecuteSqlRequest,
    ): CancelablePromise<SqlExecutionResult> {
        return __request(OpenAPI, {
            method: 'POST',
            url: '/clusters/{cluster_uuid}/{database}/sql',
            path: {
                'cluster_uuid': clusterUuid,
                'database': database,
            },
            body: requestBody,
            mediaType: 'application/json',
        });
    }
    /**
     * List notebooks
     * @returns NotebookList SQL notebooks
     * @throws ApiError
     */
    public static listNotebooks(): CancelablePromise<NotebookList> {
        return __request(OpenAPI, {
            method: 'GET',
            url: '/notebooks',
        });
    }
    /**
     * Create notebook
     * @param requestBody
     * @returns CreateNotebookResult Notebook created
     * @throws ApiError
     */
    public static createNotebook(
        requestBody: CreateNotebookRequest,
    ): CancelablePromise<CreateNotebookResult> {
        return __request(OpenAPI, {
            method: 'POST',
            url: '/notebooks',
            body: requestBody,
            mediaType: 'application/json',
        });
    }
    /**
     * Get notebook content
     * @param notebookUuid
     * @returns Notebook Notebook content
     * @throws ApiError
     */
    public static getNotebook(
        notebookUuid: string,
    ): CancelablePromise<Notebook> {
        return __request(OpenAPI, {
            method: 'GET',
            url: '/notebooks/{notebook_uuid}',
            path: {
                'notebook_uuid': notebookUuid,
            },
        });
    }
    /**
     * Delete notebook
     * @param notebookUuid
     * @returns void
     * @throws ApiError
     */
    public static deleteNotebook(
        notebookUuid: string,
    ): CancelablePromise<void> {
        return __request(OpenAPI, {
            method: 'DELETE',
            url: '/notebooks/{notebook_uuid}',
            path: {
                'notebook_uuid': notebookUuid,
            },
        });
    }
    /**
     * Create notebook cell
     * @param notebookUuid
     * @param requestBody
     * @returns CreateNotebookCellResult Notebook cell created
     * @throws ApiError
     */
    public static createNotebookCell(
        notebookUuid: string,
        requestBody: CreateNotebookCellRequest,
    ): CancelablePromise<CreateNotebookCellResult> {
        return __request(OpenAPI, {
            method: 'POST',
            url: '/notebooks/{notebook_uuid}/cells',
            path: {
                'notebook_uuid': notebookUuid,
            },
            body: requestBody,
            mediaType: 'application/json',
        });
    }
    /**
     * Update notebook cell
     * @param notebookUuid
     * @param cellUuid
     * @param requestBody
     * @returns NotebookCell Updated notebook cell
     * @throws ApiError
     */
    public static updateNotebookCell(
        notebookUuid: string,
        cellUuid: string,
        requestBody: UpdateNotebookCellRequest,
    ): CancelablePromise<NotebookCell> {
        return __request(OpenAPI, {
            method: 'PUT',
            url: '/notebooks/{notebook_uuid}/cells/{cell_uuid}',
            path: {
                'notebook_uuid': notebookUuid,
                'cell_uuid': cellUuid,
            },
            body: requestBody,
            mediaType: 'application/json',
        });
    }
    /**
     * Delete notebook cell
     * @param notebookUuid
     * @param cellUuid
     * @returns void
     * @throws ApiError
     */
    public static deleteNotebookCell(
        notebookUuid: string,
        cellUuid: string,
    ): CancelablePromise<void> {
        return __request(OpenAPI, {
            method: 'DELETE',
            url: '/notebooks/{notebook_uuid}/cells/{cell_uuid}',
            path: {
                'notebook_uuid': notebookUuid,
                'cell_uuid': cellUuid,
            },
        });
    }
    /**
     * Reorder notebook cells
     * @param notebookUuid
     * @param requestBody
     * @returns void
     * @throws ApiError
     */
    public static reorderNotebookCells(
        notebookUuid: string,
        requestBody: ReorderNotebookCellsRequest,
    ): CancelablePromise<void> {
        return __request(OpenAPI, {
            method: 'POST',
            url: '/notebooks/{notebook_uuid}/cells/order',
            path: {
                'notebook_uuid': notebookUuid,
            },
            body: requestBody,
            mediaType: 'application/json',
        });
    }
}
