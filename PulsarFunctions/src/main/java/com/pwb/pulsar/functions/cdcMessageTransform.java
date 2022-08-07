package com.pwb.pulsar.functions;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.slf4j.Logger;

public class cdcMessageTransform implements Function<GenericRecord, deviceHealthRecord> {
    @Override
    public deviceHealthRecord process(GenericRecord input, Context context) {
    	Logger LOG = context.getLogger();
   	
    	KeyValue<GenericRecord, GenericRecord> messageValue = (KeyValue<GenericRecord, GenericRecord>)input.getNativeObject();

    	LOG.info("Key is: " + printGenericRecord(messageValue.getKey()));
    	LOG.info("Value is: " + printGenericRecord(messageValue.getValue()));
    	String newRecord = 
    			createIOTDeviceMessage(printGenericRecord(messageValue.getKey()), printGenericRecord(messageValue.getValue())
    	    	.toString());
    	LOG.info("cdcMessageTransform New record is: " + newRecord);
    	deviceHealthRecord devrec = createDeviceRecord(newRecord);
    	return devrec;

 }
    public static String printGenericRecord(GenericRecord genericRecord) {
        assert (genericRecord != null);

        StringBuilder sb = new StringBuilder();

        List<Field> fields = genericRecord.getFields();

        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            sb.append(field.getName()).append(":");
            sb.append(genericRecord.getField(field));
            if (i < (fields.size() - 1)) {
                sb.append(", ");
            }
        }

        return sb.toString();
    }        
	public static String createIOTDeviceMessage(String key, String payload) {
		 StringBuilder sb = new StringBuilder();
		 String[] keyfield = key.split(Pattern.quote(":"));
		 String[] payloadfield = payload.split(Pattern.quote(":"));
		 
		 sb.append(keyfield[1]);
		 sb.append("|");
		 sb.append(payloadfield[1]);
		 return sb.toString();
	}
	public static deviceHealthRecord createDeviceRecord(String payload) {
		String[] payloadfield = payload.split(Pattern.quote("|"));
		String deviceID = payloadfield[0];
		String[] deviceTemp = payloadfield[1].split(Pattern.quote("="));
		String[] deviceBattery = payloadfield[2].split(Pattern.quote("="));
		String[] deviceLocation = payloadfield[3].split(Pattern.quote("="));
		deviceHealthRecord devRecord = new deviceHealthRecord(deviceID, deviceTemp[1], deviceBattery[1], deviceLocation[1]);
		return devRecord;
	}
}
       
