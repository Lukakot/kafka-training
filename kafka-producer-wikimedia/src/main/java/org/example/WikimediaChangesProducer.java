package org.example;

import com.launchdarkly.eventsource.EventHandler;

import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {

    public static void main(String[] args) throws InterruptedException {

        String topic = "wikimedia.recent-change";

        String url = "https://stream.wikimedia.org/v2/stream/recentchange";


        try (KafkaProducer<String, String> producer = new KafkaProducer<>(getProperties())) {

            EventHandler eventHandler = new WikimediaChangeHandler(producer, topic);
            EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(url));
            EventSource eventSource = builder.build();

            eventSource.start();

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

        //more efficient producer
        props.put(ProducerConfig.LINGER_MS_CONFIG, "20");
        props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        return props;
    }
}