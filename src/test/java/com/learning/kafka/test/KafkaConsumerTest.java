package com.learning.kafka.test;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class KafkaConsumerTest {

    private static final Logger logger = LogManager.getLogger(KafkaConsumerTest.class);

    @Test
    public void testKafkaConsumer(){
        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.181.211:9092,192.168.181.212:9092,192.168.181.213:9092");
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, "msn-group-1");

        // 是否自动提交offset.
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        // 偏移量无效之后，采取的策略
        // latest 最新的消息 （丢失消息风险）
        // earliest 上一次有效的offset （重复消费风险）
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        KafkaConsumer consumer = new KafkaConsumer(configs, new StringDeserializer(), new StringDeserializer());

        // consumer.seek(new TopicPartition("msn-topic-2", 0), 4255);

        consumer.subscribe(Arrays.asList("msn-topic-1", "msn-topic-2"), new ConsumerRebalanceListener() {

            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                // 再均衡开始之前和消费者停止消费消息之后被调用
                logger.info("消费者停止消费消息，即将触发再均衡....{}", partitions);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                // 再均衡之后和消费者开始消费之前被调用
                logger.info("已重新分配分区，消费者即将开始消费....{}", partitions);
            }
        });

        logger.info("------------------------------------------------------------------------------------------------");

        while (true){

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));

            for (String topic : Arrays.asList("msn-topic-1", "msn-topic-2")) {
                Iterator iterator = records.records(topic).iterator();
                while (iterator.hasNext()) {
                    ConsumerRecord record = (ConsumerRecord)iterator.next();
                    logger.info("topic = {}, partition = {}, offset = {}, key = {}, value = {}", topic, record.partition(), record.offset(), record.key(), record.value());
                }
            }

            try {
                // 同步提交偏移量
                // consumer.commitSync();

                // 异步提交偏移量
                consumer.commitAsync();
            }catch (Exception e){
                logger.error(e.getMessage(), e);
            }
        }
    }

}
