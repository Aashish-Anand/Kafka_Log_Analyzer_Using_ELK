package com.github.streamsLogsOnKafka.Consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Consumer {

    public static void main(String[] args) {
        new Consumer().run();
    }

    public void run() {

        String topicName = "analyze_logs";
        KafkaConsumer<String, String> consumer = KafkaConsumerConfig();

        consumer.subscribe(Arrays.asList(topicName));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(String.format("Topic: %s , Partition: %s , Value: %s", record.topic(),
                            record.partition(), record.value()));
                }
            }
        }catch(Exception e){
            System.out.println(e.getMessage());
        }finally {
            consumer.close();
        }


    }

    public KafkaConsumer<String, String> KafkaConsumerConfig() {

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"analyze-group");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        return consumer;
    }
}
