- `docs/operational-attribution-contracts.md` for the implemented resolver, writeback, reconciliation, retention, and dead-letter contract
- `docs/runbooks/attribution-completeness.md` for capture, session propagation, writeback, and resolver quality incidents

- `attribution_capture_observed`
- `tracking_dual_write_consistency`
- `shopify_writeback_observed`
- `attribution_resolver_outcome`

| `ROAS Radar * Attribution Capture Rate` | Capture completeness rate below 95% for 15 minutes | `docs/runbooks/attribution-completeness.md` |
| `ROAS Radar * Missing Session ID Rate` | Missing session id rate above 2% for 15 minutes | `docs/runbooks/attribution-completeness.md` |
| `ROAS Radar * Client Server Event Mismatch` | Dual-write mismatch rate above 5% for 10 minutes | `docs/runbooks/attribution-completeness.md` |
| `ROAS Radar * Shopify Writeback Success` | Writeback success rate below 90% for 15 minutes | `docs/runbooks/attribution-completeness.md` |
| `ROAS Radar * Resolver Unattributed Rate` | Resolver unattributed rate above 20% for 15 minutes | `docs/runbooks/attribution-completeness.md` |
