# Existing file retained; changed sections shown below.

- `docs/runbooks/ga4-fallback-rollout.md` for the staged GA4 fallback cutover process and shadow report

GA4 rollout behavior is additionally gated by `GA4_FALLBACK_ROLLOUT_MODE`:

- `off`: GA4 fallback is not applied during normal attribution processing
- `shadow`: GA4 fallback is computed, compared against the current outcome, and written to `ga4_fallback_shadow_comparisons` without changing live attribution
- `on`: GA4 fallback is applied when it is otherwise eligible
