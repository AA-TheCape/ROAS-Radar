Added platform-scoped coverage summarization after metadata resolution completes.

The resolver now emits structured coverage logs for:
- `resolutionScope: "campaign"`
- `resolutionScope: "campaign_group"`

Each log includes:
- platform
- entity type (`campaign`)
- requested count
- matched count
- resolved / fallback / unresolved counts
- resolved / fallback / unresolved rates
- bounded unresolved entity-id samples
- date window and optional source filter
