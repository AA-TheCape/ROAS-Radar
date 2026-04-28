Key changes:
- Exported `GA4_SESSION_ATTRIBUTION_PIPELINE`
- Added `ingestGa4SessionAttributionHours(...)` for explicit-hour processing
- Changed watermark completion logic so replays cannot move `watermark_hour` backward
- Kept existing `ingestGa4SessionAttribution(...)` as the planned-window path, now delegating to the explicit-hour helper
