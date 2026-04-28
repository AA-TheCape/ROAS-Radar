Added integration coverage for:
- transient failure -> retry -> successful replay without duplicate rows
- exhausted failure -> dead letter -> replay reset to pending
