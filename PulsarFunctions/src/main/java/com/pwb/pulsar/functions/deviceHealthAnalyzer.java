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

		msgSerde msgde =new msgSerde();
		deviceHealthRecord devRecord = msgde.deserialize(input);
		LOG.info("deviceHealthAnalyzer Function message received: " + devRecord.getDeviceID() + " BatteryState " + devRecord.getBatteryState());

		if(Integer.parseInt(devRecord.getBatteryState()) < 10) {
       		LOG.warn("BatteryLevel is Bad: " + devRecord.getDeviceID() + " Level is: " + devRecord.getBatteryState());	
       		return devRecord;
       	} else {
       		LOG.info("BatteryLevel is Good: " + devRecord.getDeviceID() + " Level is: " + devRecord.getBatteryState());
       		return null;
       	}
	}
}
