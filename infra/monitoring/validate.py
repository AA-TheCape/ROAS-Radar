#!/usr/bin/env python3

from __future__ import annotations

import json
import pathlib
import sys


ROOT = pathlib.Path(__file__).resolve().parent
METRICS_DIR = ROOT / "log-metrics"
ALERTS_DIR = ROOT / "alert-policies"
DASHBOARD_PATH = ROOT / "dashboard.json"


def load_json(path: pathlib.Path) -> dict:
    raw = path.read_text()
    raw = raw.replace("__ALERT_NOTIFICATION_CHANNELS__", "[]")
    return json.loads(raw)


def validate_metric(path: pathlib.Path) -> list[str]:
    data = load_json(path)
    labels = len(data.get("metricDescriptor", {}).get("labels", []))
    extractors = len(data.get("labelExtractors", {}))
    issues: list[str] = []
    if labels != extractors:
      issues.append(
            f"{path.name}: metricDescriptor.labels ({labels}) must match labelExtractors ({extractors})"
        )
    return issues


def validate_alert(path: pathlib.Path) -> list[str]:
    data = load_json(path)
    issues: list[str] = []
    conditions = data.get("conditions", [])
    if not conditions:
        issues.append(f"{path.name}: must define at least one condition")
        return issues

    has_log_match = False
    for index, condition in enumerate(conditions):
        kinds = [key for key in condition if key.startswith("condition")]
        if len(kinds) != 1:
            issues.append(
                f"{path.name}: conditions[{index}] must define exactly one condition subtype, found {kinds or 'none'}"
            )
            continue
        if kinds[0] == "conditionMatchedLog":
            has_log_match = True

    rate_limit = ((data.get("alertStrategy") or {}).get("notificationRateLimit") or {}).get("period")
    if has_log_match:
        if not rate_limit:
            issues.append(f"{path.name}: log-based alert policies require alertStrategy.notificationRateLimit.period")
    elif rate_limit:
        issues.append(f"{path.name}: only log-based alert policies may define alertStrategy.notificationRateLimit.period")

    return issues


def validate_dashboard(path: pathlib.Path) -> list[str]:
    data = load_json(path)
    issues: list[str] = []
    dashboard = data.get("mosaicLayout") or {}
    tiles = dashboard.get("tiles", [])

    if not tiles:
        return [f"{path.name}: must define at least one dashboard tile"]

    for index, tile in enumerate(tiles):
        for key in ("xPos", "yPos", "width", "height"):
            value = tile.get(key)
            if not isinstance(value, int):
                issues.append(f"{path.name}: tiles[{index}].{key} must be an integer")
                continue
            if key in {"xPos", "yPos"} and value < 0:
                issues.append(f"{path.name}: tiles[{index}].{key} must be non-negative")
            if key in {"width", "height"} and value < 1:
                issues.append(f"{path.name}: tiles[{index}].{key} must be at least 1")

        widget = tile.get("widget")
        if not isinstance(widget, dict):
            issues.append(f"{path.name}: tiles[{index}].widget must be an object")
            continue

        widget_kinds = [key for key in widget if key in {"text", "scorecard", "xyChart", "blank"}]
        if len(widget_kinds) != 1:
            issues.append(
                f"{path.name}: tiles[{index}].widget must define exactly one supported widget kind, found {widget_kinds or 'none'}"
            )

    return issues


def main() -> int:
    issues: list[str] = []

    for path in sorted(METRICS_DIR.glob("*.json")):
        issues.extend(validate_metric(path))

    for path in sorted(ALERTS_DIR.glob("*.json")):
        issues.extend(validate_alert(path))

    issues.extend(validate_dashboard(DASHBOARD_PATH))

    if issues:
        for issue in issues:
            print(issue, file=sys.stderr)
        return 1

    print("Monitoring templates validated.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
