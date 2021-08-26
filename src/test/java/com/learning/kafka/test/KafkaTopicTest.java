package com.learning.kafka.test;

import com.google.common.collect.Lists;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class KafkaTopicTest {

    @Test
    public void testCreateTopic(){
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.181.211:9092,192.168.181.212:9092,192.168.181.213:9092");
        configs.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        configs.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        AdminClient client = KafkaAdminClient.create(configs);

        client.createTopics(Lists.newArrayList(new NewTopic("msn-topic-1", 3, (short) 2), new NewTopic("msn-topic-2", 2, (short) 2)));

        client.close();
    }

}
