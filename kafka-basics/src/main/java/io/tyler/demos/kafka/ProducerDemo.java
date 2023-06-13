package io.tyler.demos.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class ProducerDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am a kafka producer");

        Properties properties = new Properties();
//        properties.setProperty("key","value");

//        This connects to Local Host
//        properties.put("bootstrap.servers", "localhost:9092");
        properties.setProperty("bootstrap.servers", "localhost:9092");

//    Example of connecting to a third party
//    properties.SetProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");
//    properties.SetProperty("security.protocol", "SASL_SSL");
//    There is more to this line just an example for now
//    properties.SetProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=")
//    properties.SetProperty("sasl.mechanism", "PLAIN")


//    set Producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

//    Create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

//        Create a Producer Record
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "java test!");

//        Send Data
        producer.send(producerRecord);

//        flush and close producer
//        the producer to send all data and block until done --synchronous
        producer.flush();

//        You can call flush if you want to but if you call close it will always flush

//        Producer close will also flush all of your data as well
        producer.close();
    }
}