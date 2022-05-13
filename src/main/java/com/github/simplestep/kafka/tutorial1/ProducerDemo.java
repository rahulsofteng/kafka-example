package com.github.simplestep.kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

    public static final String bootstrap_server = "127.0.0.1:9092";

    public static void main(String[] args) { //psvm
       // System.out.println("hello world"); //sout
        // Docemetation - https://kafka.apache.org/documentation/#producerconfigs

        //Kafka Property
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_server);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //create Producer
        Producer<String, String> producer = new KafkaProducer<String, String>(properties);
        //create record
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("first_topic", "Hello World");
        //send data
        producer.send(producerRecord);
        //flush data
        producer.flush();
        //flush producer
        producer.close();
    }
}
