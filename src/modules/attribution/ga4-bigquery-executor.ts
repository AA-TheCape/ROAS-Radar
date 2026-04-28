import { BigQuery } from '@google-cloud/bigquery';

import { type Ga4BigQueryExecutor } from './ga4-session-attribution.js';

let bigQueryClient: BigQuery | null = null;

function getBigQueryClient(): BigQuery {
  if (!bigQueryClient) {
    bigQueryClient = new BigQuery();
  }

  return bigQueryClient;
}

export function createGa4BigQueryExecutor(location: string): Ga4BigQueryExecutor {
  return {
    async runQuery(input) {
      const [rows] = await getBigQueryClient().query({
        location,
        query: input.query,
        params: input.params,
        useLegacySql: false
      });

      return rows as unknown[];
    }
  };
}
