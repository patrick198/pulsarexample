package com.pwb.pulsar.functions;

import java.util.regex.Pattern;

import org.apache.pulsar.functions.api.SerDe;

public class msgSerde implements SerDe<deviceHealthRecord> {
	@Override
	public deviceHealthRecord deserialize(byte[] input) {
		String s = new String(input);
        String[] fields = s.split(Pattern.quote("|"));
        return new deviceHealthRecord(fields[0], fields[1]); 
	}

	@Override
	public byte[] serialize(deviceHealthRecord input) {

		return "%s|%s".format(input.getDeviceID(), input.getBatteryState()).getBytes();		
	}

}
