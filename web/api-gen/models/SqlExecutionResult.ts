/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { SqlRow } from './SqlRow';
export type SqlExecutionResult = {
    columns: Array<string>;
    rows: Array<SqlRow>;
    commandTag: string;
    rowsAffected: number;
    /**
     * SQL execution error returned as a normal response payload.
     */
    error?: string;
};

