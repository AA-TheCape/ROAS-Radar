# Attribution Policy Release Checklist

Use this checklist for release notes, deployment sign-off, and promotion records whenever a release changes attribution resolver behavior, attribution tiers, order attribution storage, Meta evidence handling, reporting/API attribution fields, dashboard attribution displays, or attribution backfills.

Every applicable release artifact must reference `docs/attribution-policy-v2.md` as the source of truth for V2 attribution policy semantics.

## Required Checklist

- [ ] Reference `docs/attribution-policy-v2.md` in the PR, release notes, or promotion record.
- [ ] Resolver rule version: identify the active resolver version, note whether `attribution_resolver_v2` behavior changes, and describe compatibility with prior resolver output.
- [ ] Migrations: list forward migrations, rollbacks, compatibility expectations for existing rows, and any required migration ordering.
- [ ] API fields: list added, removed, renamed, or reinterpreted attribution fields and note client compatibility expectations.
- [ ] Dashboard and reporting: list changes to tier filters, labels, metrics, aggregate semantics, and analyst interpretation.
- [ ] Backfill behavior: document scope, idempotency, historical-row treatment, retry behavior, operator controls, and expected completion evidence.
- [ ] Auditability: identify decision artifacts, evidence fields, logs, metrics, run records, or audit tables that prove attribution decisions and backfills.
- [ ] Known limitations: document unresolved edge cases, data gaps, non-goals, and any cases where Meta evidence remains parallel-only instead of canonical.
