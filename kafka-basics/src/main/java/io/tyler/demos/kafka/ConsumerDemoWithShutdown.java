package io.tyler.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;


public class ConsumerDemoWithShutdown {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithShutdown.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am a Kafka Consumer");

        String groupId = "my-java-application";
        String topic = "demo_batman";

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


//    Create Consumer configs
//        You Deserializer will go off of the data that it is for Example: JSON,AVRO or String Deserializers
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        properties.setProperty("group.id", groupId);

//        None offset = If we don't have any existing consumer group we will fail/ Must set the consumer offset group before you start the application
//        Earliest = Read from the beginning of the topic
//        Latest = Read from the latest or most recent value/ End of the log
        properties.setProperty("auto.offset.reset", "earliest");

//        Create a Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

//        get a reference to the main thread
//        This is a reference to the current Thread that is running my Program.
        final Thread mainThread = Thread.currentThread();

//        adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(){
            public  void run(){
                log.info("Detected a shutdown, let's exit by calling conusmer.wakeup()");
                consumer.wakeup();


//                Join the main thread to allow the execution of the code in the main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        try {


//        Subscribe to a topic/ You can do this in different ways either a pattern or you can do as a collection with Arrays as lists
            consumer.subscribe(Arrays.asList(topic));

//        poll the data
            while (true) {
                log.info("Polling");

//            Duration of == How long will your consumer wait until it recieves Data from Kafka before moving on
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

//          This line is for every record within my collections of records do something with it
                for (ConsumerRecord<String, String> record : records) {
                    log.info("Key: " + record.key() + ", Value: " + record.value());
                    log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                }
            }
        } catch (WakeupException e){
//            This catch/exception is expected because we want it to shutdown
            log.info("Consumer is starting to shut down");
        } catch (Exception e){
//            This catch/Exception is if something goes wrong.
            log.error("Unexpected exception in the consumer", e);
        } finally {
//            This will close the consumer, this will also commit the offsets
            consumer.close();
            log.info("The consumer is now gracefully shut down");
        }
    }
}