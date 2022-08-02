package com.pwb.pulsar.functions;

import java.util.regex.Pattern;

import org.apache.pulsar.functions.api.SerDe;

public class msgSerde implements SerDe<deviceHealthRecord> {
	@Override
	public deviceHealthRecord deserialize(byte[] input) {
		String s = new String(input);
        String[] fields = s.split(Pattern.quote("|"));
        return new deviceHealthRecord(fields[0], fields[1], fields[2], fields[3]); 
	}

	@Override
	public byte[] serialize(deviceHealthRecord input) {
//		return "%s|%s|%s|%s".format(input.getDeviceID(),input.getDeviceTemp(),input.getBatteryState(),input.getDeviceLocation()).getBytes();		
    	StringBuilder sb = new StringBuilder();
    	sb.append(input.getDeviceID());
    	sb.append("|");
    	sb.append(input.getDeviceTemp());
    	sb.append("|");
    	sb.append(input.getBatteryState());
    	sb.append("|");
    	sb.append(input.getDeviceLocation());
    	return sb.toString().getBytes();
	}

}
