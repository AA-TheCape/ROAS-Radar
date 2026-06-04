import type { OrderRow } from './api';
import { formatConfidenceScore } from './format';

type ConfidenceOrder = Pick<OrderRow, 'attributionTier' | 'confidenceScore' | 'lastAttributionRunAt'>;

export type ConfidenceDisplay = {
	label: string;
	detail: string;
	state: 'scored' | 'unattributed' | 'pending' | 'unavailable';
};

export function getConfidenceDisplay(order: ConfidenceOrder | null | undefined): ConfidenceDisplay {
	if (!order) {
		return {
			label: 'Not available',
			detail: 'No order confidence fields were returned.',
			state: 'unavailable'
		};
	}

	if (order.confidenceScore !== null && order.confidenceScore !== undefined && Number.isFinite(order.confidenceScore)) {
		const isUnattributed = order.attributionTier === 'unattributed' && order.confidenceScore === 0;

		return {
			label: formatConfidenceScore(order.confidenceScore),
			detail: isUnattributed ? 'Unattributed by backend' : 'Backend confidence score',
			state: isUnattributed ? 'unattributed' : 'scored'
		};
	}

	if (!order.lastAttributionRunAt) {
		return {
			label: 'Pending',
			detail: 'Awaiting attribution run',
			state: 'pending'
		};
	}

	return {
		label: 'Unavailable',
		detail: 'Legacy or unavailable confidence',
		state: 'unavailable'
	};
}
