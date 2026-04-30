render_template() {
  sed \
    -e "s|__ENVIRONMENT__|$(escape_for_sed "$ENVIRONMENT")|g" \
    -e "s|__RUNBOOK_URL_INGESTION__|$(escape_for_sed "$(build_runbook_url "ingestion-failures.md")")|g" \
    -e "s|__RUNBOOK_URL_META_ORDER_VALUE__|$(escape_for_sed "$(build_runbook_url "meta-order-value-ingestion.md")")|g" \
    -e "s|__RUNBOOK_URL_ATTRIBUTION__|$(escape_for_sed "$(build_runbook_url "attribution-worker-backlog.md")")|g" \
    -e "s|__RUNBOOK_URL_ATTRIBUTION_COMPLETENESS__|$(escape_for_sed "$(build_runbook_url "attribution-completeness.md")")|g" \
    -e "s|__RUNBOOK_URL_API_LATENCY__|$(escape_for_sed "$(build_runbook_url "api-latency.md")")|g" \
    -e "s|__RUNBOOK_URL_DATA_QUALITY__|$(escape_for_sed "$(build_runbook_url "identity-data-quality.md")")|g" \
    -e "s|__ALERT_NOTIFICATION_CHANNELS__|$notification_channels_json|g" \
    -e "s|__DASHBOARD_DISPLAY_NAME__|$(escape_for_sed "$OBSERVABILITY_DASHBOARD_DISPLAY_NAME")|g"
}
