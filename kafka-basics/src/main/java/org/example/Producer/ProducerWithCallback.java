package org.example.Producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithCallback {
    private static final Logger log = LoggerFactory.getLogger(ProducerWithCallback.class.getSimpleName());

    public static void main(String[] args) {

        log.info("I am a kafka producer !");

        Properties props = getProperties();

        try(KafkaProducer<String,String> producer = new KafkaProducer<>(props)){
            for (int i = 0; i < 10; i++) {
                ProducerRecord<String,String> producerRecord = new ProducerRecord<>("demo_java", "Hello world " + i);
                producer.send(producerRecord, (metadata, exception) -> {
                    //executes every time a record is successfully sent or an exception is thrown
                    if(exception == null) {
                        log.info("Received new metadata/ \n" +
                                "Topic: " + metadata.topic() + "\n" +
                                "Partition: " + metadata.partition() + "\n" +
                                "Offset: " + metadata.offset() + "\n" +
                                "Timestamp: " + metadata.timestamp());
                    } else {
                        log.error("Error while producing", exception);
                    }
                });
            }
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