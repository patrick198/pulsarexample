package com.pwb.pulsar.functions;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.slf4j.Logger;

public class messageAnalyzer implements Function<String, String> {
    @Override
    public String process(String input, Context context) {
        try {
    	Logger LOG = context.getLogger();
//        String messageId = new String(context.getMessageId());
        String messageId = new String(context.getFunctionName());
        if (input.contains("Battery=0")) {
            LOG.warn("A warning was received in message ID: " + messageId);
            return String.format("%s","HEALTH:BAD " + input);
        } else {
            LOG.info("Message {} received with Content: {}", messageId, input);
            return String.format("%s","HEALTH:GOOD " + input);
        }

//        return String.format("%s",input);
    }
       catch (Exception e) {
		e.printStackTrace();
  }
		return null;
 }
}

// CREATE TABLE IF NOT EXISTS cdctest.tbl1 (key text PRIMARY KEY, c1 text);
// INSERT INTO cdctest.tbl1 (key,c1) VALUES ('ID001','delivery-online');
