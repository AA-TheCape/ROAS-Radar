## Summary

<!-- Describe the user-visible or operational change. -->

## Verification

<!-- List tests, migrations, smoke checks, or manual validation. -->

## Attribution Policy Review

If this PR changes attribution resolver behavior, attribution tiers, order attribution storage, Meta evidence handling, reporting/API attribution fields, dashboard attribution displays, or attribution backfills, reference `docs/attribution-policy-v2.md` and complete this checklist.

- [ ] `docs/attribution-policy-v2.md` is referenced in this PR, or this PR does not change attribution policy.
- [ ] Resolver rule version impact is documented, including whether `attribution_resolver_v2` behavior changes.
- [ ] Migration changes are listed, including forward migration, rollback coverage, and compatibility for existing rows.
- [ ] API field changes are listed, including added, removed, renamed, or reinterpreted attribution fields.
- [ ] Dashboard and reporting changes are listed, including tier filters, labels, metrics, and analyst interpretation.
- [ ] Backfill behavior is documented, including scope, idempotency, historical-row treatment, and operator controls.
- [ ] Auditability is documented, including decision artifacts, evidence fields, logs, metrics, or run records.
- [ ] Known limitations are documented, including unresolved edge cases and any cases where Meta evidence remains parallel-only.
