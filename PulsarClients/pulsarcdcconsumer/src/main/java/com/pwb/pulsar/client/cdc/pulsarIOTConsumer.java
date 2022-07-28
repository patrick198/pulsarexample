package com.pwb.pulsar.client.cdc;


import java.util.List;
import java.util.regex.Pattern;

import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;

public class pulsarIOTConsumer {
	private static final String YOUR_PULSAR_TOKEN = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE2NTgyNjQ4MTQsImlzcyI6ImRhdGFzdGF4Iiwic3ViIjoiY2xpZW50OzRhYmY1MzA4LWE1MTUtNGZhOS1iYWVjLTk0ZDE5M2U2ZjA5YztZMlJqYzNSeVpXRnRjdz09OzE0ZTg1YzY5MDkiLCJ0b2tlbmlkIjoiMTRlODVjNjkwOSJ9.I4B_a-_3_HBzsg_QDbbpzg2GJKOvYbh019i__zgIY32hbmyRuG6edGQG2-ovyO9VFZDdsF8pR-PvIsTT-WqdmxQwNLINDX8BAjanEPvRbIs60cHr7P_rLseQYNqCAFj8CXD70T8xG-tzl8a5tNjN_rG4gZUBwKHDanL1HDUWPX0BTgk1gjcglQ66yxohHnbM4mvWmspiOONswWvki7kd0ixsTS51iA60KVh8tB4lIPSVX_k3blwbWezWBroibTMjnzdU2vySHvdIRcJMueBNsu4K5ixOQdevXymvU0X4Xh8HursWSuyVoAnMwxawJN-SnShPH2ipa3a9vxRpfFWOwg";
    private static final String SERVICE_URL = "pulsar+ssl://pulsar-gcp-useast1.streaming.datastax.com:6651";

	public static void main(String[] args) throws Exception {
        // Create client object
        PulsarClient pulsarClient = PulsarClient.builder()
                .serviceUrl(SERVICE_URL)
                .authentication(AuthenticationFactory.token(YOUR_PULSAR_TOKEN))
                .build();
    	
        Schema<KeyValue<GenericRecord, GenericRecord>> iotSensorSchema = Schema.KeyValue(
        		Schema.AUTO_CONSUME(),
        		Schema.AUTO_CONSUME(),
                KeyValueEncodingType.SEPARATED
            ); 

        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic("persistent://cdcstreams/cdc-iot/devicestatus")
                .create();

        Consumer<KeyValue<GenericRecord, GenericRecord>> consumer = pulsarClient.newConsumer(iotSensorSchema)
                .topic("cdcstreams/astracdc/data-c4feb514-af8a-4cc0-823a-2fa0fd9e6b60-cdctest.tbl1")
                .subscriptionName("cdc-sub")
                .subscribe();
//        consumer.seek(MessageId.earliest);
        
        boolean receiveMsg = false;
        do {
        	Message<?> message = consumer.receive();
        	
            KeyValue<GenericRecord, GenericRecord> messageValue =
                    (KeyValue<GenericRecord, GenericRecord>)message.getValue();
            GenericRecord msgKey = messageValue.getKey();
            GenericRecord msgPayload = messageValue.getValue();
            System.out.println("Received message ");
            System.out.println("  key: " + printGenericRecord(msgKey));
            System.out.println("  properties: " + message.getProperties());
            System.out.println("  payload:  " + printGenericRecord(msgPayload));
            
            consumer.acknowledge(message);
            receiveMsg = false;
//            String[] fields = printGenericRecord(msgPayload).split(Pattern.quote("|"));
//            System.out.println("Fields: " + fields[0] + " " + fields[1] + " " + fields[2]);
            producer.send(printGenericRecord(msgPayload).getBytes());
            producer.flush();
            
        } while (!receiveMsg);

        consumer.close();
		producer.close();
		pulsarClient.close();
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

}
