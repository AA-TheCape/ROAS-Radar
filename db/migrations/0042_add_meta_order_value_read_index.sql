BEGIN;

CREATE INDEX meta_ads_order_value_aggregates_org_report_date_campaign_idx
  ON meta_ads_order_value_aggregates (organization_id, report_date DESC, campaign_id);

COMMIT;
