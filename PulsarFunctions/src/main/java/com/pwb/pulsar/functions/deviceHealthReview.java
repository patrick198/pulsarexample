package com.pwb.pulsar.functions;
import java.util.regex.Pattern;

import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.slf4j.Logger;

public class deviceHealthReview implements Function<String, String> {
    @Override
    public String process(String input, Context context) {
    	Logger LOG = context.getLogger();
//        String messageId = new String(context.getMessageId());
        String messageId = new String(context.getFunctionName());
        deviceHealthRecord devRecord = createIOTDeviceMessage(input);
       	
       	if(Integer.parseInt(devRecord.getBatteryState()) < 10) {
       		LOG.warn("BatteryLevel is Bad: " + devRecord.getDeviceID() + " Level is: " + devRecord.getBatteryState());
       		return String.format(devRecord.getDeviceID() + "|" + devRecord.getBatteryState() + "|BadBatteryState");
       	} else {
       		LOG.info("BatteryLevel is Good: " + devRecord.getDeviceID() + " Level is: " + devRecord.getBatteryState());
       		return null;
       	}
   }
	public static deviceHealthRecord createIOTDeviceMessage(String payload) {
		String[] payloadfield = payload.split(Pattern.quote("|"));
		String deviceID = payloadfield[0];
		String[] deviceTemp = payloadfield[1].split(Pattern.quote("="));
		String[] deviceBattery = payloadfield[2].split(Pattern.quote("="));
		String[] deviceLocation = payloadfield[3].split(Pattern.quote("="));
		deviceHealthRecord devRecord = new deviceHealthRecord(deviceID, deviceTemp[1], deviceBattery[1], deviceLocation[1]);
		return devRecord;
	}    
}

// CREATE TABLE IF NOT EXISTS cdctest.tbl1 (key text PRIMARY KEY, c1 text);
// INSERT INTO cdctest.tbl1 (key,c1) VALUES ('ID001','delivery-online');
