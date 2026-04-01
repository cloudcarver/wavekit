/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { NotebookCellType } from './NotebookCellType';
export type UpdateNotebookCellRequest = {
    cellType: NotebookCellType;
    clusterUuid?: string | null;
    database?: string | null;
    backgroundDdl?: boolean;
    content: string;
};

