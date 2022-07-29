package com.pwb.pulsar.functions;

import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.apache.pulsar.functions.api.SerDe;
import org.slf4j.Logger;

public class deviceHealthAnalyzer implements  Function<String, deviceHealthRecord>  {

	@Override
	public deviceHealthRecord process(String input, Context context) throws Exception {
		Logger LOG = context.getLogger();
		LOG.info("deviceHealthAnalyzer Function message received");
		return null;
	}

}
