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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.core.KafkaTemplate;
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
@EmbeddedKafka(partitions = 3, brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092", "partitions=3"}, topics = "parallel-topic")
public class ConsumerTest {



    @Autowired
    KafkaTemplate<String, String> mockProducer;

    @SpyBean
    ParralelConsumers parralelConsumers;

    @BeforeEach
    void send() {


        int i=1;
        while (i <= 300) {

            mockProducer.send(new ProducerRecord<>("parallel-topic", String.valueOf(i), String.valueOf(i)));
            mockProducer.flush();
            log.info("sent : " + i);
            i++;
        }

    }



    @Test
    public void testConsumer()  {

        doCallRealMethod().when(parralelConsumers).listen1(isA(ConsumerRecord.class));
        doCallRealMethod().when(parralelConsumers).listen2(isA(ConsumerRecord.class));
        doCallRealMethod().when(parralelConsumers).listen3(isA(ConsumerRecord.class));


        //round-robin approach doesnt ensure each batch gets evenly distributed (i.e. != 100,100,100)
        verify(parralelConsumers, atLeast(80)).listen1(isA(ConsumerRecord.class));
        verify(parralelConsumers, atLeast(80)).listen2(isA(ConsumerRecord.class));
        verify(parralelConsumers, atLeast(80)).listen3(isA(ConsumerRecord.class));

    }
}
