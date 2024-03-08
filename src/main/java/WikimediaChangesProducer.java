import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {

    public static void main(String[] args) throws InterruptedException {

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(getProperties())) {

            String topic = "wikimedia.recent-change";

            EventHandler eventHandler = new WikimediaChangeHandler(producer, topic);

            String url = "https://stream.wikimedia.org/v2/stream/recentchange";

            EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(url));
            EventSource eventSource = builder.build();

            // start producer in another thread
            eventSource.start();

            // we produce for 10 minutes and block the program until then
            TimeUnit.MINUTES.sleep(10);
        }
    }


    private static Properties getProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "https://concise-narwhal-5503-eu2-kafka.upstash.io:9092");
        props.put("sasl.mechanism", "SCRAM-SHA-256");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"Y29uY2lzZS1uYXJ3aGFsLTU1MDMkW03-mxy6pl_Gybo1QXxTFfoH0XF7lwtgbx8\" password=\"OTA0M2Y1YmYtMWY3My00YmMwLWE3ZjYtZDJhNWEzZTU3YTgy\";");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }
}
