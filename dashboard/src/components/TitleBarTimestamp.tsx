import React, { useEffect, useMemo, useState } from "react";

import { formatCurrentTimestamp } from "../lib/format";

type TitleBarTimestampProps = {
	getNow?: () => Date;
	scheduleInterval?: typeof setInterval;
	clearScheduledInterval?: typeof clearInterval;
	scheduleTimeout?: typeof setTimeout;
	clearScheduledTimeout?: typeof clearTimeout;
};

export default function TitleBarTimestamp({
	getNow = () => new Date(),
	scheduleInterval = setInterval,
	clearScheduledInterval = clearInterval,
	scheduleTimeout = setTimeout,
	clearScheduledTimeout = clearTimeout,
}: TitleBarTimestampProps) {
	const [currentTime, setCurrentTime] = useState(() => getNow());

	useEffect(() => {
		let minuteInterval: ReturnType<typeof setInterval> | null = null;
		let minuteTimeout: ReturnType<typeof setTimeout> | null = null;

		const startMinuteInterval = () => {
			setCurrentTime(getNow());
			minuteInterval = scheduleInterval(() => {
				setCurrentTime(getNow());
			}, 60_000);
		};

		const now = getNow();
		const msUntilNextMinute =
			60_000 - (now.getSeconds() * 1000 + now.getMilliseconds());
		minuteTimeout = scheduleTimeout(startMinuteInterval, msUntilNextMinute);

		return () => {
			if (minuteTimeout) {
				clearScheduledTimeout(minuteTimeout);
			}

			if (minuteInterval) {
				clearScheduledInterval(minuteInterval);
			}
		};
	}, [
		clearScheduledInterval,
		clearScheduledTimeout,
		getNow,
		scheduleInterval,
		scheduleTimeout,
	]);

	const labels = useMemo(
		() => ({
			local: formatCurrentTimestamp(currentTime, { includeTimeZoneName: true }),
			utc: formatCurrentTimestamp(currentTime, {
				timeZone: "UTC",
				includeTimeZoneName: true,
			}).replace(/ UTC$/, ""),
		}),
		[currentTime],
	);

	return (
		<div
			aria-label="Current timestamp"
			className="space-y-1 border-t border-line/60 pt-3 text-caption text-ink-muted"
		>
			<p className="font-semibold uppercase tracking-[0.14em] text-teal">
				Current time
			</p>
			<p>{labels.local}</p>
			<p>UTC {labels.utc}</p>
		</div>
	);
}
