Upgraded the PG pool to use:
- max/min pool sizing
- idle and connection timeouts
- maxUses
- keepalive
- optional SSL
- statement/query timeouts
- pool error logging
- a reusable `checkDatabaseHealth()` helper for `/readyz`
