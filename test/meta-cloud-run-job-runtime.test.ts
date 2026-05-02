test("resolveMetaAdsRuntimeDescriptor rejects mixed-mode Cloud Run execution", () => {
	process.env.META_ADS_JOB_MODE = "order_value";

	assert.throws(
		() => resolveMetaAdsRuntimeDescriptor("spend"),
		/Meta Ads runtime mode mismatch: expected spend, received order_value/,
	);
});

test("Cloud Run deploy script keeps spend and order-value job entrypoints isolated", async () => {
	assert.match(spendSection, /--args=run,meta-ads:sync:start/);
	assert.match(spendSection, /META_ADS_JOB_MODE=spend/);
	assert.doesNotMatch(spendSection, /META_ADS_ORDER_VALUE_SYNC_ENABLED=/);

	assert.match(orderValueSection, /--args=run,meta-ads:order-value:start/);
	assert.match(orderValueSection, /META_ADS_JOB_MODE=order_value/);
	assert.match(orderValueSection, /META_ADS_ORDER_VALUE_SYNC_ENABLED=/);
});
