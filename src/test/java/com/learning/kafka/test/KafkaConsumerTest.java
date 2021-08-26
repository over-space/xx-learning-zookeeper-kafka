package com.learning.kafka.test;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class KafkaConsumerTest {

    @Test
    public void testKafkaConsumer(){
        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.181.211:9092,192.168.181.212:9092,192.168.181.213:9092");
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, "msn-group-1");

        KafkaConsumer consumer = new KafkaConsumer(configs, new StringDeserializer(), new StringDeserializer());

        consumer.subscribe(Arrays.asList("msn-topic-1", "msn-topic-2"));

        while (true){

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));

            for (String topic : Arrays.asList("msn-topic-1", "msn-topic-2")) {
                Iterator iterator = records.records(topic).iterator();
                while (iterator.hasNext()) {
                    ConsumerRecord record = (ConsumerRecord)iterator.next();
                    System.out.printf("topic = %s, partition = %d, offset = %d, key = %s, value = %s\n", topic, record.partition(), record.offset(), record.key(), record.value());
                }
            }
        }
    }

}
