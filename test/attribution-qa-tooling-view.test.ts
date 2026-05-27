import assert from "node:assert/strict";
import test from "node:test";

import {
	attributionQaPayloadV1SuccessFixture,
	normalizeAttributionQaPayloadV1,
	type AttributionQaPayloadV1,
} from "../packages/attribution-schema/index.js";
import {
	click,
	h,
	loadDashboardModule,
	mountUi,
	tick,
} from "./dashboard-ui-test-helpers";

test("attribution QA tooling renders sanitized candidate and raw payload details", async () => {
	const { default: AttributionQaToolingView } = await loadDashboardModule<{
		default: (props: {
			selectedOrderId: string | null;
			reportingTimezone: string;
			qaPayloadSection: {
				data: {
					orderId: string;
					source: "persisted_snapshot" | "generated_on_read";
					payload: AttributionQaPayloadV1;
				} | null;
				loading: boolean;
				error: string | null;
			};
		}) => unknown;
	}>("dashboard/src/components/AttributionQaToolingView.tsx");

	const payload = normalizeAttributionQaPayloadV1({
		...attributionQaPayloadV1SuccessFixture,
		order: {
			...attributionQaPayloadV1SuccessFixture.order,
			identifiers: {
				...attributionQaPayloadV1SuccessFixture.order.identifiers,
				checkout_token: "checkout-token-secret",
				cart_token: "cart-token-secret",
				email_hash:
					"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
			},
		},
		candidates: {
			...attributionQaPayloadV1SuccessFixture.candidates,
			deterministic_first_party:
				attributionQaPayloadV1SuccessFixture.candidates.deterministic_first_party.map(
					(candidate) => ({
						...candidate,
						click_id_value: "GCLID-FRONTEND-SECRET",
					}),
				),
			ga4_fallback: [
				{
					candidate_group: "ga4_fallback",
					source_key: "ga4-client-999-session-888",
					touchpoint_id: "ga4-client-999-session-888",
					session_id: null,
					source_touch_event_id: "ga4-event-secret",
					occurred_at_utc: "2026-04-30T11:45:00Z",
					source: "google",
					medium: "cpc",
					campaign: "ga4-brand",
					content: "hero",
					term: "widget",
					click_id_type: "gclid",
					click_id_value: "GA4-GCLID-FRONTEND-SECRET",
					match_source: "ga4_fallback",
					attribution_reason: "ga4_fallback_match",
					confidence_score: 0.35,
					confidence_label: "low",
					is_direct: false,
					is_synthetic: true,
					selected: false,
				},
			],
		},
		credits: attributionQaPayloadV1SuccessFixture.credits.map((credit) => ({
			...credit,
			click_id_value: "GCLID-CREDIT-SECRET",
		})),
		explainability: [
			...attributionQaPayloadV1SuccessFixture.explainability,
			{
				run_id: "523e4567-e89b-42d3-a456-426614174444",
				order_id: "shopify-order-1105",
				touchpoint_id: "ga4-client-999-session-888",
				model_key: null,
				explain_stage: "eligibility_filter",
				decision: "excluded",
				decision_reason: "blocked_by_deterministic_first_party_winner",
				details_json: {
					landing_url:
						"https://store.example/products/widget?access_token=url-secret&utm_source=google",
				},
				order_occurred_at_utc: "2026-04-30T12:00:00Z",
				created_at_utc: "2026-04-30T12:30:00Z",
			},
		],
	});

	const mounted = await mountUi(
		h(AttributionQaToolingView, {
			selectedOrderId: "shopify-order-1105",
			reportingTimezone: "UTC",
			qaPayloadSection: {
				data: {
					orderId: "shopify-order-1105",
					source: "persisted_snapshot",
					payload,
				},
				loading: false,
				error: null,
			},
		}),
	);

	try {
		assert.match(mounted.container.textContent ?? "", /Winner rationale/);
		assert.match(mounted.container.textContent ?? "", /GA4 fallback details/);
		assert.match(mounted.container.textContent ?? "", /ga4_fallback_candidate_1/);

		const button = Array.from(
			mounted.container.querySelectorAll<HTMLButtonElement>("button"),
		).find((element) => element.textContent?.includes("Show raw payload"));
		assert.ok(button);
		click(button);
		await tick();

		const rendered = mounted.container.textContent ?? "";
		assert.doesNotMatch(rendered, /checkout-token-secret/);
		assert.doesNotMatch(rendered, /cart-token-secret/);
		assert.doesNotMatch(
			rendered,
			/cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc/,
		);
		assert.doesNotMatch(rendered, /GCLID-FRONTEND-SECRET/);
		assert.doesNotMatch(rendered, /GCLID-CREDIT-SECRET/);
		assert.doesNotMatch(rendered, /GA4-GCLID-FRONTEND-SECRET/);
		assert.doesNotMatch(rendered, /ga4-client-999-session-888/);
		assert.doesNotMatch(rendered, /ga4-event-secret/);
		assert.doesNotMatch(rendered, /url-secret/);
	} finally {
		mounted.cleanup();
	}
});
