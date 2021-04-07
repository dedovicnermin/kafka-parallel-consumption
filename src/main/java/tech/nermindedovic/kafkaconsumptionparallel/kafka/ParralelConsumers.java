package tech.nermindedovic.kafkaconsumptionparallel.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class ParralelConsumers {


    @KafkaListener(topicPartitions = {@TopicPartition(topic = "parallel-topic", partitions = "0")}, groupId = "parallel-app")
    public void listen1(ConsumerRecord<String, String> record) {
        log.info("partition{}: <{}> : <{}> FROM listen1",record.partition(), record.key(), record.topic());
    }

    @KafkaListener(topicPartitions = {@TopicPartition(topic = "parallel-topic", partitions = "1")}, groupId = "parallel-app")
    public void listen2(ConsumerRecord<String, String> record) {
        log.info("partition{}: <{}> : <{}> FROM listen2",record.partition(), record.key(), record.topic());
    }

    @KafkaListener(topicPartitions = {@TopicPartition(topic = "parallel-topic", partitions = "2")}, groupId = "parallel-app")
    public void listen3(ConsumerRecord<String, String> record) {
        log.info("partition{}: <{}> : <{}> FROM listen3",record.partition(), record.key(), record.topic());
    }




}
