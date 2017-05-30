package com.ddp.util;

import com.ddp.access.BaseRequest;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.producer.KafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Created by eguo on 5/29/17.
 */
public class KafkaUtils {
    private static io.vertx.kafka.client.producer.KafkaProducer<String, BaseRequest> createProducerJava(Vertx vertx
                                                                                                        , String kafkaBrokers) {
        // creating the producer using map and class types for key and value serializers/deserializers
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers);
        config.put(ProducerConfig.ACKS_CONFIG, "1");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, com.ddp.util.BaseRequestSerializer.class);

        // use producer for interacting with Apache Kafka
        io.vertx.kafka.client.producer.KafkaProducer<String, BaseRequest> producer = io.vertx.kafka.client.producer.KafkaProducer.create(vertx, config, String.class, BaseRequest.class);
        return producer;
    }

    private static io.vertx.kafka.client.consumer.KafkaConsumer<String, BaseRequest> createConsumerJava(Vertx vertx, String kafkaBrokers, String groupId) {
        // creating the consumer using properties config
        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BaseRequestDeserializer.class);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        // use consumer for interacting with Apache Kafka
        io.vertx.kafka.client.consumer.KafkaConsumer<String, BaseRequest> consumer = io.vertx.kafka.client.consumer.KafkaConsumer.create(vertx, config);
        return consumer;
    }

    private static  io.vertx.kafka.client.consumer.KafkaConsumer<String, BaseRequest> createConsumerJavaRest(Vertx vertx, String kafkaBrokers, String groupId) {

        // creating the consumer using properties config
        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,kafkaBrokers);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BaseRequestDeserializer.class);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        // use consumer for interacting with Apache Kafka
        io.vertx.kafka.client.consumer.KafkaConsumer<String, BaseRequest> consumer = io.vertx.kafka.client.consumer.KafkaConsumer.create(vertx, config);
        return consumer;
    }
}
