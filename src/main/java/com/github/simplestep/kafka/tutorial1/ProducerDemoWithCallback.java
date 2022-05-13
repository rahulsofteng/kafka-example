package com.github.simplestep.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    public static final String bootstrap_server = "127.0.0.1:9092";
    public static final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

    public static void main(String[] args) { //psvm
       // System.out.println("hello world"); //sout
        // Docemetation - https://kafka.apache.org/documentation/#producerconfigs

        for(int i=0; i<10; i++) {
            logger.info("Hello, inserting data: "+ "hello-"+i);
            String topic = "first_topic";
            String key = "id_" + Integer.toString(i);
            String value = "Hello World"+Integer.toString(i);
            //Kafka Property
            Properties properties = new Properties();
            properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_server);
            properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            //create Producer
            Producer<String, String> producer = new KafkaProducer<String, String>(properties);
            //create record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic,key, value);
            logger.info("key: "+key); //log the key
            //send data
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("Received new metadata: \n"+
                                "Topic : "+recordMetadata.topic() + "\n" +
                                "Partition : " + recordMetadata.partition() + "\n" +
                                "Offset : " + recordMetadata.offset() + "\n" +
                                "Timestamp : " + recordMetadata.timestamp());
                    } else {
                        logger.info("something bad happened: ", e.getStackTrace());
                    }
                }
            });
            //flush data
            producer.flush();
            //flush producer
            producer.close();
        }
    }
}
