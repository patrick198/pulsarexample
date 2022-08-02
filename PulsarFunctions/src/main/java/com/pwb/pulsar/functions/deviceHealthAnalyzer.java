package com.pwb.pulsar.functions;

import java.util.regex.Pattern;

import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.apache.pulsar.functions.api.SerDe;
import org.slf4j.Logger;

public class deviceHealthAnalyzer implements  Function<byte[], deviceHealthRecord>  {

	@Override
	public deviceHealthRecord process(byte[] input, Context context) throws Exception {
		Logger LOG = context.getLogger();

//		LOG.info("deviceHealthAnalyzer New raw msg input converted to string is: " + input + " Raw length: " + input.length());
		msgSerde msgde =new msgSerde();
		deviceHealthRecord devrec = msgde.deserialize(input);
//		LOG.info("deviceHealthAnalyzer Function message received: " + input.getDeviceID() + " BatteryState " + input.getBatteryState());
		LOG.info("deviceHealthAnalyzer Function message received: " + devrec.getDeviceID() + " BatteryState " + devrec.getBatteryState());
		return devrec;
	}

	public static deviceHealthRecord createIOTDeviceMessage(String payload) {
		String[] payloadfield = payload.split(Pattern.quote("|"));
		deviceHealthRecord devRecord = new deviceHealthRecord(payloadfield[0], payloadfield[1], payloadfield[2], payloadfield[3]);
		return devRecord;
	}

}
