package com.pwb.pulsar.client;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;

public class PulsarIOTConsumer {
	private static final String YOUR_PULSAR_TOKEN = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE2NTgyNjQ4MTQsImlzcyI6ImRhdGFzdGF4Iiwic3ViIjoiY2xpZW50OzRhYmY1MzA4LWE1MTUtNGZhOS1iYWVjLTk0ZDE5M2U2ZjA5YztZMlJqYzNSeVpXRnRjdz09OzE0ZTg1YzY5MDkiLCJ0b2tlbmlkIjoiMTRlODVjNjkwOSJ9.I4B_a-_3_HBzsg_QDbbpzg2GJKOvYbh019i__zgIY32hbmyRuG6edGQG2-ovyO9VFZDdsF8pR-PvIsTT-WqdmxQwNLINDX8BAjanEPvRbIs60cHr7P_rLseQYNqCAFj8CXD70T8xG-tzl8a5tNjN_rG4gZUBwKHDanL1HDUWPX0BTgk1gjcglQ66yxohHnbM4mvWmspiOONswWvki7kd0ixsTS51iA60KVh8tB4lIPSVX_k3blwbWezWBroibTMjnzdU2vySHvdIRcJMueBNsu4K5ixOQdevXymvU0X4Xh8HursWSuyVoAnMwxawJN-SnShPH2ipa3a9vxRpfFWOwg";
    private static final String SERVICE_URL = "pulsar+ssl://pulsar-gcp-useast1.streaming.datastax.com:6651";

	public static void main(String[] args) throws ClassNotFoundException, IOException {
        PulsarClient pulsarClient = PulsarClient.builder()
                .serviceUrl(SERVICE_URL)
                .authentication(AuthenticationFactory.token(YOUR_PULSAR_TOKEN))
                .build();
        // Create consumer on a topic with a subscription
        Consumer consumer = pulsarClient.newConsumer()
                .topic("cdcstreams/iotdevicemetrics/bad-devices")
                .subscriptionName("test-sub")
                .subscribe();

        boolean receivedMsg = false;
        do {
            // Block for up to 1 second for a message
            Message msg = consumer.receive(1, TimeUnit.SECONDS);

            if(msg != null){
            	deviceHealthRecord devrec =  deserialize(msg.getData());
                System.out.println("\n \n WARN Device Battery BAD state - DeviceID: " + devrec.getDeviceID() + " BatteryState: " + devrec.getBatteryState() + "\n \n");
                // Acknowledge the message to remove it from the message backlog
                consumer.acknowledge(msg);
 //               receivedMsg = true;
            }

        } while (!receivedMsg);

        //Close the consumer
        consumer.close();

        // Close the client
        pulsarClient.close();

	}
	public static deviceHealthRecord deserialize(byte[] input) {
		String s = new String(input);
        String[] fields = s.split(Pattern.quote("|"));
        return new deviceHealthRecord(fields[0], fields[1], fields[2], fields[3]); 
	}
}
