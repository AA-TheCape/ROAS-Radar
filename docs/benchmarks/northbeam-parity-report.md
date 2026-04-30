# Northbeam parity report

- Fixture set: `2026-04-30-v1`
- Orders benchmarked: `7`
- Models benchmarked: `first_touch`, `last_touch`, `last_non_direct`, `linear`, `clicks_only`, `hinted_fallback_only`

## Model slice

| Model | Severity | Actual revenue | Reference revenue | Delta | Delta % | Winner mismatch | Touchpoint mismatch | Unattributed delta |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| first_touch | RED | 570.00 | 570.00 | 0.00 | 0% | 14.29% | 0% | 0 (0pp) |
| last_touch | RED | 570.00 | 570.00 | 0.00 | 0% | 28.57% | 0% | 0 (0pp) |
| last_non_direct | RED | 570.00 | 570.00 | 0.00 | 0% | 28.57% | 0% | 0 (0pp) |
| linear | RED | 570.00 | 570.00 | 0.00 | 0% | 14.29% | 0% | 0 (0pp) |
| clicks_only | RED | 570.00 | 570.00 | 0.00 | 0% | 28.57% | 0% | 0 (0pp) |
| hinted_fallback_only | RED | 80.00 | 0.00 | 80.00 | 100% | 14.29% | 14.29% | -1 (14.29pp) |

## Channel slice

| Model | Channel | Severity | Actual revenue | Reference revenue | Delta | Delta % | Order delta |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| last_touch | google / cpc | RED | 0.00 | 110.00 | 110.00 | 100% | -1 |
| first_touch | google / paid_search | RED | 0.00 | 90.00 | 90.00 | 100% | -1 |
| last_non_direct | google / paid_search | RED | 0.00 | 90.00 | 90.00 | 100% | -1 |
| linear | google / paid_search | RED | 0.00 | 45.00 | 45.00 | 100% | -1 |
| clicks_only | google / paid_search | RED | 0.00 | 90.00 | 90.00 | 100% | -1 |
| last_touch | impact / affiliate | RED | 0.00 | 150.00 | 150.00 | 100% | -1 |
| last_non_direct | impact / affiliate | RED | 0.00 | 150.00 | 150.00 | 100% | -1 |
| clicks_only | impact / affiliate | RED | 0.00 | 150.00 | 150.00 | 100% | -1 |
| last_touch | meta / paid_social | RED | 260.00 | 0.00 | 260.00 | 100% | 2 |
| last_non_direct | meta / paid_social | RED | 260.00 | 0.00 | 260.00 | 100% | 2 |
| clicks_only | meta / paid_social | RED | 260.00 | 0.00 | 260.00 | 100% | 2 |
| hinted_fallback_only | meta / paid_social | RED | 80.00 | 0.00 | 80.00 | 100% | 1 |
| first_touch | meta / paid_social | RED | 110.00 | 0.00 | 110.00 | 100% | 1 |
| first_touch | direct / none | RED | 210.00 | 120.00 | 90.00 | 75% | 1 |
| last_non_direct | direct / none | RED | 210.00 | 120.00 | 90.00 | 75% | 1 |
| clicks_only | direct / none | RED | 210.00 | 120.00 | 90.00 | 75% | 1 |
| first_touch | google / cpc | RED | 100.00 | 210.00 | 110.00 | 52.38% | -1 |
| last_non_direct | google / cpc | RED | 100.00 | 210.00 | 110.00 | 52.38% | -1 |
| clicks_only | google / cpc | RED | 100.00 | 210.00 | 110.00 | 52.38% | -1 |
| linear | direct / none | RED | 243.33 | 198.33 | 45.00 | 22.69% | 0 |
| last_touch | direct / none | GREEN | 310.00 | 310.00 | 0.00 | 0% | 0 |
| linear | google / cpc | GREEN | 121.67 | 121.67 | 0.00 | 0% | 0 |
| first_touch | impact / affiliate | GREEN | 150.00 | 150.00 | 0.00 | 0% | 0 |
| linear | impact / affiliate | GREEN | 75.00 | 75.00 | 0.00 | 0% | 0 |
| linear | meta / paid_social | GREEN | 130.00 | 130.00 | 0.00 | 0% | 0 |

## Cohort slice

| Cohort | Model | Severity | Orders | Winner mismatch | Direct mismatch | Lookback mismatch | Synthetic fallback mismatch |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| direct_only | first_touch | GREEN | 1 | 0% | 0% | 0% | 0% |
| direct_only | last_touch | GREEN | 1 | 0% | 0% | 0% | 0% |
| direct_only | last_non_direct | GREEN | 1 | 0% | 0% | 0% | 0% |
| direct_only | linear | GREEN | 1 | 0% | 0% | 0% | 0% |
| direct_only | clicks_only | GREEN | 1 | 0% | 0% | 0% | 0% |
| direct_only | hinted_fallback_only | GREEN | 1 | 0% | 0% | 0% | 0% |
| mixed_click_view | first_touch | GREEN | 1 | 0% | 0% | 0% | 0% |
| mixed_click_view | last_touch | GREEN | 1 | 0% | 0% | 0% | 0% |
| mixed_click_view | last_non_direct | GREEN | 1 | 0% | 0% | 0% | 0% |
| mixed_click_view | linear | GREEN | 1 | 0% | 0% | 0% | 0% |
| mixed_click_view | clicks_only | GREEN | 1 | 0% | 0% | 0% | 0% |
| mixed_click_view | hinted_fallback_only | GREEN | 1 | 0% | 0% | 0% | 0% |
| click_id_only_missing_utms | first_touch | GREEN | 1 | 0% | 0% | 0% | 0% |
| click_id_only_missing_utms | last_touch | GREEN | 1 | 0% | 0% | 0% | 0% |
| click_id_only_missing_utms | last_non_direct | GREEN | 1 | 0% | 0% | 0% | 0% |
| click_id_only_missing_utms | linear | GREEN | 1 | 0% | 0% | 0% | 0% |
| click_id_only_missing_utms | clicks_only | GREEN | 1 | 0% | 0% | 0% | 0% |
| click_id_only_missing_utms | hinted_fallback_only | GREEN | 1 | 0% | 0% | 0% | 0% |
| identity_journey_fallback | first_touch | GREEN | 1 | 0% | 0% | 0% | 0% |
| identity_journey_fallback | last_touch | RED | 1 | 100% | 0% | 0% | 0% |
| identity_journey_fallback | last_non_direct | RED | 1 | 100% | 0% | 0% | 0% |
| identity_journey_fallback | linear | GREEN | 1 | 0% | 0% | 0% | 0% |
| identity_journey_fallback | clicks_only | RED | 1 | 100% | 0% | 0% | 0% |
| identity_journey_fallback | hinted_fallback_only | GREEN | 1 | 0% | 0% | 0% | 0% |
| shopify_hint_fallback | first_touch | GREEN | 1 | 0% | 0% | 0% | 0% |
| shopify_hint_fallback | last_touch | GREEN | 1 | 0% | 0% | 0% | 0% |
| shopify_hint_fallback | last_non_direct | GREEN | 1 | 0% | 0% | 0% | 0% |
| shopify_hint_fallback | linear | GREEN | 1 | 0% | 0% | 0% | 0% |
| shopify_hint_fallback | clicks_only | GREEN | 1 | 0% | 0% | 0% | 0% |
| shopify_hint_fallback | hinted_fallback_only | RED | 1 | 100% | 100% | 100% | 100% |
| same_timestamp_tie | first_touch | RED | 1 | 100% | 0% | 0% | 0% |
| same_timestamp_tie | last_touch | RED | 1 | 100% | 0% | 0% | 0% |
| same_timestamp_tie | last_non_direct | RED | 1 | 100% | 0% | 0% | 0% |
| same_timestamp_tie | linear | RED | 1 | 100% | 0% | 0% | 0% |
| same_timestamp_tie | clicks_only | RED | 1 | 100% | 0% | 0% | 0% |
| same_timestamp_tie | hinted_fallback_only | GREEN | 1 | 0% | 0% | 0% | 0% |
| no_eligible_touches | first_touch | GREEN | 1 | 0% | 0% | 0% | 0% |
| no_eligible_touches | last_touch | GREEN | 1 | 0% | 0% | 0% | 0% |
| no_eligible_touches | last_non_direct | GREEN | 1 | 0% | 0% | 0% | 0% |
| no_eligible_touches | linear | GREEN | 1 | 0% | 0% | 0% | 0% |
| no_eligible_touches | clicks_only | GREEN | 1 | 0% | 0% | 0% | 0% |
| no_eligible_touches | hinted_fallback_only | GREEN | 1 | 0% | 0% | 0% | 0% |

## Top variance drivers

- RED clicks_only / identity_journey_fallback / identity-journey-fallback: winner_mismatch. Actual winner `meta-late`, reference winner `affiliate-early`.
- RED last_non_direct / identity_journey_fallback / identity-journey-fallback: winner_mismatch. Actual winner `meta-late`, reference winner `affiliate-early`.
- RED last_touch / identity_journey_fallback / identity-journey-fallback: winner_mismatch. Actual winner `meta-late`, reference winner `affiliate-early`.
- RED clicks_only / same_timestamp_tie / same-timestamp-tie: winner_mismatch. Actual winner `meta-landing`, reference winner `google-checkout`.
- RED first_touch / same_timestamp_tie / same-timestamp-tie: winner_mismatch. Actual winner `meta-landing`, reference winner `google-checkout`.
- RED last_non_direct / same_timestamp_tie / same-timestamp-tie: winner_mismatch. Actual winner `meta-landing`, reference winner `google-checkout`.
- RED last_touch / same_timestamp_tie / same-timestamp-tie: winner_mismatch. Actual winner `meta-landing`, reference winner `google-checkout`.
- RED linear / same_timestamp_tie / same-timestamp-tie: winner_mismatch. Actual winner `meta-landing`, reference winner `google-checkout`.
- RED hinted_fallback_only / shopify_hint_fallback / shopify-hint-fallback: winner_mismatch, allocation_status_mismatch, touchpoint_count_mismatch, synthetic_fallback_usage_mismatch, direct_classification_mismatch. Actual winner `hint-meta`, reference winner `none`.

## Thresholds

- Green: model revenue delta <= 1.0%, channel delta <= 2.0%, winner mismatch <= 3.0%, unattributed delta <= 0.5pp.
- Yellow: model revenue delta <= 3.0%, channel delta <= 5.0%, winner mismatch <= 8.0%, unattributed delta <= 1.5pp.
- Red: values above yellow thresholds.

