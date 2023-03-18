package com.example.kafka;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Arrays;
import java.util.Properties;

public class ConsumerUsingJava {

    public static void main(String[] args) {

        // Step 1- Consume the records from all topic
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, ConstantConfigurations.appID);
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ConstantConfigurations.bootStrapServer );
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "CONSUMER_GROUP1");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //Step  2 - Set object of the kafka Consumer

        KafkaConsumer<Integer, String> consumer = new KafkaConsumer<Integer, String>(consumerProps);
        consumer.subscribe(Arrays.asList("all_orders"));

        // Step 3- Produce the records from all topic

        Properties producerProps = new Properties();
        // Passing app id / name from ConstantConfigurations java File
        producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, ConstantConfigurations.appID);
        // Passing bootstrap server from ConstantConfigurations java File
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ConstantConfigurations.bootStrapServer);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        //Step 4 - Set object of the kafka Producer

        KafkaProducer<Integer, String> producer = new KafkaProducer<Integer,String>(producerProps);

        //Step 5 - Calling the send method on this Consumer object

        while(true){

            ConsumerRecords<Integer, String> records = consumer.poll(100);
            for(ConsumerRecord<Integer,String> record: records) {

//                if(record.value().split(",")[3].equals("CLOSED")) {
                if(record.value().contains("CLOSED")) {

                    producer.send(new ProducerRecord<Integer,String>("closed_orders", record.key(),record.value()));
                } else {
                    producer.send(new ProducerRecord<Integer,String>("completed_orders", record.key(),record.value()));

                }

            }

        }
    }
}
