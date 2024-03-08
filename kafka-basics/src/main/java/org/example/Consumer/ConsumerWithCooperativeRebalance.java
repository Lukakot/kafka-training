package org.example.Consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.example.Producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerWithCooperativeRebalance {

    private static final Logger log = LoggerFactory.getLogger(Producer.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Kafka consumer.");



        //create consumer
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(getProperties())) {


            //get a reference to the current thread;
            final Thread mainThread = Thread.currentThread();

            //adding shutdown hook
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
                consumer.wakeup();
                //join the main thread to allow the execution of the code in the main thread
                try{
                    mainThread.join();
                }catch (InterruptedException e){
                    e.printStackTrace();
                }
            }));


            //subscribe consumer to our topic(s)
            consumer.subscribe(Collections.singleton("demo_java"));

            try{
                while(true){
                    ConsumerRecords<String, String> records =
                            consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record: records){
                        log.info("Key: " + record.key() + ", value: " + record.value());
                        log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                    }
                }
            }catch (WakeupException e){
                log.info("Wake up exception!");
            }catch (Exception e){
                log.error("Unexpected exception");
            } finally {
                // this will also commit the offsets if needed
                consumer.close();
                log.info("Consumer is gracefully closed.");
            }
        }

    }


    private static Properties getProperties() {

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "https://concise-narwhal-5503-eu2-kafka.upstash.io:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "my-second-application");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());
        props.put("sasl.mechanism", "SCRAM-SHA-256");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"Y29uY2lzZS1uYXJ3aGFsLTU1MDMkW03-mxy6pl_Gybo1QXxTFfoH0XF7lwtgbx8\" password=\"OTA0M2Y1YmYtMWY3My00YmMwLWE3ZjYtZDJhNWEzZTU3YTgy\";");
        return props;
    }
}
