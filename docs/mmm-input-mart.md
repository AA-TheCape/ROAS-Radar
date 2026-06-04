# MMM Daily Input Mart

`mmm_daily_input_mart_v1` is the deterministic daily export surface for marketing mix modeling inputs. `mmm_weekly_channel_input_mart_v1` is the approved weekly x channel modeling table derived from it.

The mart is versioned in the table name and `mart_version` column. Version `v1` has two row types:

- `paid_media`: native ad platform rows from deterministic spend projections. These rows preserve platform, connection id, account/campaign/ad set/ad/creative ids and names, source/medium/campaign/content/term taxonomy, spend, impressions, clicks, currency, and spend freshness. `attribution_model` is `none`.
- `attribution`: Shopify outcome and attribution credit rows aggregated by source/medium/campaign/content/term and attribution model. These rows preserve order counts, Shopify revenue, fractional credit orders/revenue, new/returning customer credit splits, match-source coverage, confidence-label coverage, Shopify freshness, and attribution freshness.

The split row model avoids duplicating Shopify attribution metrics across multiple native ad entities that share the same taxonomy. MMM consumers can aggregate by taxonomy for modeled response variables while still retaining native id rows for paid media spend audits and platform-level reconciliation.

Refresh entry points:

- `refreshDailyMmmInputMart(client, metricDates)` rebuilds specific dates.
- `refreshAllDailyMmmInputMart(client)` rebuilds all dates observed in Shopify orders, Meta spend, or Google Ads spend.
- `refreshWeeklyMmmChannelInputMartWithClient(client, { startDate, endDate, attributionModels })` rebuilds weekly x channel rows from the daily mart.

The weekly mart aggregates each Monday-starting week by canonical source, medium, campaign, channel, and channel group. Each row includes:

- media inputs: spend, impressions, clicks
- outcome inputs: Shopify orders and revenue
- deterministic anchors: attribution credit orders/revenue, new/returning splits, match-source coverage, and confidence-label coverage
- controls: week of year, month, quarter, and simple holiday-window flags
- data quality reports: missing dimension checks, spend-without-delivery checks, outcome-without-credit checks, and leakage checks proving no source daily row exceeds the row's week end date

Standalone regeneration:

```bash
npm run mmm:refresh-weekly -- --start-date 2026-04-01 --end-date 2026-04-30 --attribution-models last_touch
```

## Baseline Model Training

The first MMM training pipeline is `baseline_linear_mmm_v1`.

Freeze the selected calibration window before training. The freeze command refreshes the weekly mart, writes an immutable `mmm_baseline_calibration_freezes` row, persists row counts, deterministic attribution coverage, freshness metrics, campaign metadata coverage, exposure coverage, data-quality check results, aggregate totals, frozen rows, and a stable evidence hash.

```bash
npm run mmm:freeze-baseline -- --start-date 2026-04-01 --end-date 2026-04-30 --attribution-model last_touch --status approved --approved-by marketing-ops
```

Then train with the approved freeze id:

```bash
npm run mmm:train-baseline -- --start-date 2026-04-01 --end-date 2026-04-30 --attribution-model last_touch --freeze-id <approved-freeze-id>
```

For Cloud Run scheduling, the same trainer reads `MMM_BASELINE_LOOKBACK_DAYS`, `MMM_BASELINE_LAG_DAYS`, `MMM_BASELINE_FREEZE_ID`, `MMM_BASELINE_SUBMITTED_BY`, and optional model tuning environment variables. The checked-in deployment contract runs `roas-radar-mmm-baseline-<environment>` weekly after attribution materialization so baseline model outputs are promoted through the same staging and production path as the rest of the pipeline.

The trainer requires `approved_freeze_id` for `baseline_linear_mmm_v1`, validates that the freeze is approved and matches the requested calibration window plus attribution model, and trains only from the frozen `snapshot_rows`. Later mart refreshes do not affect the training input for that run:

- weekly channel rows become media features using `log1p(adstock(spend))`.
- weekly Shopify outcomes provide the response for the selected attribution model.
- Per-segment deterministic attribution metrics are persisted in `calibration_report` and `validation_report`; they are not used as direct per-channel replacement labels.

Each completed baseline run also writes an auditable calibration governance report to `calibration_report`:

- `governance.channelWeekReconciliation` reconciles each modeled channel/week against deterministic `attribution_credit_revenue` from the same weekly mart snapshot.
- `modeledRevenue` is computed as `max(0, fitted coefficient * transformed spend feature)` for the channel/week. Non-selected paid media is reconciled under `__other_paid__`.
- `divergenceRate` is `abs(modeledRevenue - deterministicAnchorRevenue) / max(abs(deterministicAnchorRevenue), 1)`.
- Deterministic tiers are `aligned`, `watch`, and `alert`. Defaults are `watch >= 25%` divergence and `alert >= 50%` divergence.
- `governance.divergenceAlerts` and top-level `divergenceAlerts` contain every channel/week alert breach.

Operators can override thresholds per run with `--calibration-warn-divergence-rate` and `--calibration-alert-divergence-rate`, or the `MMM_BASELINE_CALIBRATION_WARN_DIVERGENCE_RATE` and `MMM_BASELINE_CALIBRATION_ALERT_DIVERGENCE_RATE` environment variables.

## Bayesian Hierarchical MMM

`bayesian_hierarchical_mmm_v1` trains from `mmm_weekly_channel_input_mart_v1` using a dependency-free Gibbs sampler in the Node Cloud Run job. The model response is weekly total Shopify revenue from the mart outcome columns. Paid channel features are geometric adstock followed by Hill saturation; controls include a linear weekly trend and annual weekly Fourier sine/cosine terms.

The production trainer uses a conjugate Gaussian hierarchy:

- channel coefficients are sampled from channel-group Normal effects
- channel-group effects are sampled around a global media prior
- control coefficients use zero-centered Normal priors
- residual variance is sampled from an inverse-gamma conditional posterior

Deterministic attribution credit is retained as a calibration anchor only. It informs prior centering and the persisted calibration report, but it is not used as a per-channel replacement label.

Each completed Bayesian run persists `posteriorCoefficients`, `posteriorGroupEffects`, `posteriorSigma`, aggregate channel contribution intervals, weekly x channel contribution intervals, calibration inputs, and diagnostics through `mmm_model_runs`. `run_config.posteriorEngine = gibbs_sampler_conjugate_gaussian_hierarchical_v1` identifies the inference path.

Diagnostics are computed from retained Markov chains, not from synthetic independent draws. The run fails before completion when R-hat exceeds `1.1`, minimum effective sample size is below `100`, or posterior contribution sanity checks fail. Failed diagnostics therefore keep the run out of production-ready API readiness.

Cloud Run assumptions: use Node 22, at least 1 vCPU, and 1 GiB memory for default `MMM_BAYESIAN_POSTERIOR_CHAINS=4`, `MMM_BAYESIAN_POSTERIOR_DRAWS=1000`, `MMM_BAYESIAN_POSTERIOR_WARMUP_DRAWS=500`, and up to 12 selected channels. Increase timeout, CPU, or reduce `MMM_BAYESIAN_MAX_CHANNELS` before increasing chains/draws on wide channel windows. Use `MMM_BAYESIAN_RANDOM_SEED` only for reproducible validation runs.

Completed model runs are stored in `mmm_model_runs` with versioned `run_config`, `input_summary`, `model_artifact`, `calibration_report`, `validation_report`, and `approved_freeze_id`. Every completed run also writes immutable row-level inputs to `mmm_model_run_input_snapshots` with `snapshot_version = mmm_weekly_channel_snapshot_v1`, per-row hashes, and a run-level snapshot hash in `input_summary`. Baseline training fails when the approved freeze contains DQ status `fail`; warning counts and freeze evidence are retained in `input_summary`.

## Read API and Export

The approved mart is exposed through `GET /api/reporting/mmm`.

Completed MMM runs and their persisted calibration reports are exposed through `GET /api/reporting/mmm/model-runs`. The admin MMM readiness panel renders the calibration governance status, divergence alert counts, thresholds, and reconciliation logic from this API so reconciliation decisions can be audited without direct database access.

Taxonomy drift readiness is exposed through `GET /api/reporting/mmm/taxonomy-drift`. Marketing Ops and Data Platform should run this report before training or refreshing modeled inputs so unknown taxonomy values and unresolved metadata do not silently enter MMM features.

Authentication matches the other reporting APIs: callers must send the configured bearer token or an authenticated app session. Responses include `X-ROAS-Radar-MMM-Schema: mmm_daily_input_mart_v1`, and JSON responses also include `schemaVersion`.

Supported query parameters:

- `startDate` and `endDate` are required `YYYY-MM-DD` dates.
- `martRowType` optionally filters to `paid_media` or `attribution`.
- `attributionModel` optionally filters attribution rows to a supported attribution model.
- `platform` optionally filters to `meta`, `google`, or `taxonomy`.
- `source` and `campaign` optionally filter canonical taxonomy dimensions.
- `limit` and `offset` page export rows. `limit` defaults to `1000` and is capped at `10000`.
- `format=json|csv` defaults to JSON. CSV responses repeat `schemaVersion`, `generationTimestamp`, and `readinessStatus` on each row for model-training export jobs.

JSON responses include:

- `readiness.status`: `ready` when every requested date has matching mart rows, `partial` when only some dates do, and `not_ready` when none do.
- `readiness.generationTimestamp`: the latest `last_computed_at` value across matching mart rows in the requested window.
- `readiness.excludedDateWindows`: date windows excluded from the filtered training set. Reasons are `no_mmm_mart_rows` when the mart has no rows for the date, or `no_rows_matching_filters` when the mart has rows but none match the requested filters.
- `rows`: schema-versioned mart rows in camelCase with native paid-media ids, taxonomy dimensions, metrics, freshness timestamps, and coverage JSON.

### Taxonomy Drift Report

`GET /api/reporting/mmm/taxonomy-drift` supports the same date, mart row type, attribution model, platform, source, and campaign filters as the MMM export endpoint. It also accepts:

- `staleAfterDays`, default `14`, to flag native campaign metadata whose `ad_platform_entity_metadata.last_seen_at` is older than the requested `endDate` minus the threshold.
- `sampleLimit`, default `10`, capped at `50`, to return the top grouped examples for each drift category.

Responses use `schemaVersion: mmm_taxonomy_drift_report_v1` and include:

- `overall` and `daily` count/rate summaries for unknown source, unmapped source, unknown-or-unmapped source, unknown medium, unmapped medium, unknown-or-unmapped medium, unresolved campaign metadata, and stale campaign metadata.
- `nativeIdCoverage` for MMM rows eligible for platform-native IDs, including account, campaign, ad set, ad, creative, and account-plus-campaign join key coverage.
- `samples` grouped by source, medium, campaign, platform, row type, attribution model, account id, and campaign id. Sample categories include `unknown_or_unmapped_source`, `unknown_or_unmapped_medium`, `unresolved_campaign_metadata`, `stale_campaign_metadata`, and `missing_platform_native_campaign_key`.
