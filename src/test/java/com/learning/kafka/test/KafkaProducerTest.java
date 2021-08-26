package com.learning.kafka.test;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class KafkaProducerTest {


    @Test
    public void testKafkaProducer() throws InterruptedException {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.181.211:9092,192.168.181.212:9092,192.168.181.213:9092");
        configs.put(ProducerConfig.ACKS_CONFIG, "0");

        KafkaProducer producer = new KafkaProducer(configs, new StringSerializer(), new StringSerializer());

        ExecutorService executorService = Executors.newFixedThreadPool(2);

        executorService.execute(() -> {
            int i = 0;
            while (true) {
                if(i % 2 == 0) {
                    producer.send(new ProducerRecord("msn-topic-1", "key-" + i,"hello kafka-" + i));
                    try {
                        TimeUnit.MILLISECONDS.sleep(300);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                i++;
            }
        });

        executorService.execute(() -> {
            int i = 0;
            while (true) {
                if (i % 2 != 0) {
                    producer.send(new ProducerRecord("msn-topic-2", "key-" + i, "hello kafka-" + i));
                    try {
                        TimeUnit.MILLISECONDS.sleep(300);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                i++;
            }
        });

        while(true){
            TimeUnit.MILLISECONDS.sleep(600);
        }
    }
}
