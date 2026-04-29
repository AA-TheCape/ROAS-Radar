import { BigQuery } from "@google-cloud/bigquery";
let bigQueryClient = null;
function getBigQueryClient() {
    if (!bigQueryClient) {
        bigQueryClient = new BigQuery();
    }
    return bigQueryClient;
}
export function createGa4BigQueryExecutor(location) {
    return {
        async runQuery(input) {
            const [rows] = await getBigQueryClient().query({
                location,
                query: input.query,
                params: input.params,
                useLegacySql: false,
            });
            return rows;
        },
    };
}
