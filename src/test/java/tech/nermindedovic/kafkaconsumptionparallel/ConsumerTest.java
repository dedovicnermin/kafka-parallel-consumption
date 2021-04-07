package tech.nermindedovic.kafkaconsumptionparallel;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import tech.nermindedovic.kafkaconsumptionparallel.kafka.ParralelConsumers;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@SpringBootTest
@Slf4j
@DirtiesContext
@ExtendWith(value = MockitoExtension.class)
@EnableAutoConfiguration
//@EmbeddedKafka(partitions = 3, brokerProperties = {"listeners=PLAINTEXT://localhost:9092,PLAINTEXT://localhost:9093,PLAINTEXT://localhost:9094", "ports=9092,9093,9094"})
@EmbeddedKafka(partitions = 3, brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092", "partitions=3"})
public class ConsumerTest {


    @BeforeEach
    void send() {
        MockProducer<String, String> mockProducer = new MockProducer<>(Cluster.bootstrap(Arrays.asList(new InetSocketAddress("localhost", 9092))), true, new DefaultPartitioner(), new StringSerializer(), new StringSerializer());
        int i=1;
        while (i <= 300) {
            mockProducer.send(new ProducerRecord<>("parallel-topic", String.valueOf(i), String.valueOf(i)));
            log.info("sent : " + i);
            i++;
        }
    }


    @Test
    public void testConsumer()  {



        ParralelConsumers parralelConsumers = Mockito.mock(ParralelConsumers.class);

        doNothing().when(parralelConsumers).listen1(any(ConsumerRecord.class));
        doNothing().when(parralelConsumers).listen2(any(ConsumerRecord.class));
        doNothing().when(parralelConsumers).listen3(any(ConsumerRecord.class));


        verify(parralelConsumers, atLeast(100)).listen1(any());
        verify(parralelConsumers, atLeast(100)).listen2(any());
        verify(parralelConsumers, atLeast(100)).listen3(any());


    }
}
