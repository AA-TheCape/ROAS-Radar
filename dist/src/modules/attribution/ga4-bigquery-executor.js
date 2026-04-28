"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.createGa4BigQueryExecutor = createGa4BigQueryExecutor;
const bigquery_1 = require("@google-cloud/bigquery");
let bigQueryClient = null;
function getBigQueryClient() {
    if (!bigQueryClient) {
        bigQueryClient = new bigquery_1.BigQuery();
    }
    return bigQueryClient;
}
function createGa4BigQueryExecutor(location) {
    return {
        async runQuery(input) {
            const [rows] = await getBigQueryClient().query({
                location,
                query: input.query,
                params: input.params,
                useLegacySql: false
            });
            return rows;
        }
    };
}
