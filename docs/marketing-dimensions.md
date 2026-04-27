# Marketing Dimension Canonicalization

ROAS Radar now applies one shared normalization layer for touchpoints, attribution outputs, and ad-platform spend rows.

## Canonical fields

- `source`
- `medium`
- `campaign`
- `content`
- `term`

## Platform-specific identifiers retained

Spend ETL keeps the native IDs already stored in the warehouse:

- Meta: `account_id`, `campaign_id`, `adset_id`, `ad_id`, `creative_id`
- Google Ads: `account_id`, `campaign_id`, `adset_id` (ad group), `ad_id`, `creative_id`
- Touchpoints and attribution: `gclid`, `fbclid`, `ttclid`, `msclkid`

Those IDs remain the primary breakdown keys when a creative-level or platform-native audit is needed.

## Shared normalization rules

### 1. Text cleanup

- Trim leading and trailing whitespace.
- Lowercase canonical marketing dimensions.
- Collapse internal whitespace to a single space for free-text dimensions.
- Normalize source and medium aliases by converting punctuation and separators to `_`.

### 2. Canonical source mapping

Known aliases map to:

- `google`: `google`, `google_ads`, `googleadwords`, `adwords`, `youtube`
- `meta`: `facebook`, `fb`, `instagram`, `ig`, `meta`, `meta_ads`
- `tiktok`: `tiktok`, `tik_tok`, `tt`
- `microsoft`: `bing`, `microsoft`, `microsoft_ads`, `msn`
- `email`: `email`, `newsletter`, `klaviyo`, `mailchimp`
- `direct`: `direct`, `(direct)`
- `referral`: `referral`
- `organic`: `organic`, `organic_search`, `seo`
- `shopify`: `shopify`

If a source value is present but does not match a known alias, ROAS Radar stores `unmapped`.

If no source is present and no click ID can infer one, touchpoint storage keeps `NULL` so direct and untagged traffic are not misclassified during attribution. Spend rows use `unknown` instead because they must remain reportable dimensions.

### 3. Canonical medium mapping

Known aliases map to:

- `cpc`: `cpc`, `ppc`, `sem`, `paidsearch`, `paid_search`
- `paid_social`: `paidsocial`, `paid_social`, `social_paid`, `social`
- `email`: `email`, `newsletter`
- `organic_search`: `organic`, `organic_search`, `seo`
- `referral`: `referral`
- `affiliate`: `affiliate`
- `display`: `display`, `banner`, `programmatic`
- `sms`: `sms`, `text`
- `direct`: `direct`, `none`, `(none)`

If a medium value is present but does not map cleanly, ROAS Radar stores `unmapped`.

If no medium is present and no click ID can infer one, touchpoint storage keeps `NULL`. Spend rows use `unknown`.

### 4. Click ID inference

When UTM source or medium is missing, click IDs backfill the canonical channel:

- `gclid` => `source=google`, `medium=cpc`
- `fbclid` => `source=meta`, `medium=paid_social`
- `ttclid` => `source=tiktok`, `medium=paid_social`
- `msclkid` => `source=microsoft`, `medium=cpc`

### 5. Campaign, content, and term

- `campaign`, `content`, and `term` remain free-text dimensions after cleanup.
- Missing touchpoint values stay `NULL`.
- Missing spend values are written as `unknown`.
- Spend ETL maps `content` from the most creative-specific label available:
  - Meta creative rows: `creative_name`, then `ad_name`
  - Google creative rows: `ad_name`
  - Non-creative spend rows: `unknown`

## Where normalization is applied

- Tracking ingestion writes canonical touchpoint dimensions into `tracking_sessions` and `tracking_events`.
- Attribution resolution normalizes touchpoint dimensions again before writing `attribution_order_credits` and `attribution_results`, so older or partially normalized session data does not leak through.
- Meta and Google spend ETL write canonical fields into `meta_ads_daily_spend` and `google_ads_daily_spend` alongside native platform IDs and names. Those tables are derived reporting projections; exact upstream rows stay in `meta_ads_raw_spend_records` and `google_ads_raw_spend_records`.
- GA4 session attribution ingestion keeps canonical `source`, `medium`, `content`, and `term` from GA4 raw export, but can enrich `campaign`, `campaign_id`, `account_id`, `account_name`, and Google Ads channel fields from linked Google Ads transfer tables when a campaign join succeeds.

## GA4 linked Google Ads enrichment

When GA4 export includes Google Ads-linked campaign identifiers, ROAS Radar joins those identifiers to Google Ads transfer `Campaign` tables to fill campaign metadata.

- Join key precedence: `session_traffic_source_last_click.google_ads_campaign.campaign_id`, then `session_traffic_source_last_click.manual_campaign.campaign_id`
- Campaign text precedence: Google Ads transfer `campaign_name`, then GA4 `google_ads_campaign.campaign_name`, then GA4 raw/manual campaign text
- Account precedence: Google Ads transfer `customer_id` and `customer_descriptive_name`, then GA4 `google_ads_campaign.customer_id` and `account_name`
- Channel precedence: Google Ads transfer `campaign_advertising_channel_type` and `campaign_advertising_channel_sub_type`
- Unresolved joins remain nullable and are tagged with lineage `unresolved`

Lineage is persisted separately for campaign, account, and channel metadata so analysts can distinguish values that came from GA4 raw export versus Ads-linked transfer tables.

## Unknown vs unmapped

- `unknown`: the dimension was absent and could not be inferred.
- `unmapped`: a value was present, but it did not match the supported canonical taxonomy.

This distinction is intentional so reporting can separate instrumentation gaps from taxonomy drift.
