package io.tyler.demos.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class ProducerDemoWithCallback {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());

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

//        Batch size to low for production better to keep the default Kafka size
        properties.setProperty("batch.size", "400");
//        This makes every batch I send out go to a different partition but this is not recommended for production
//        properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());

//    Create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

//        By running a for loop and producing multiple messages in quick succesion it defaults to a stickyPartioner
//        In other words it batches the messages and sends a group to the same partition rather than sending it
//        Round Robin Style

        for (int j=0;  j<10; j++ ){
//            Create a Producer Record
            for (int i=0; i<30 ; i++){

//            Create a Producer Record
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "Hello world" + i);

//        Send Data
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
//                Executes every time a record successfully sent or an exception is thrown
                        if (e == null){
                            log.info("Received new metadata \n" +
                                    "Topic:" + metadata.topic() + "\n" +
                                    "Partition:" + metadata.partition() + "\n" +
                                    "Offset:" + metadata.offset() + "\n" +
                                    "Timestamp:" + metadata.timestamp());
                        }else{
                            log.error("Error while producing", e);
                        }
                    }
                });
        }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }


        }



//
//        flush and close producer
//        the producer to send all data and block until done --synchronous
        producer.flush();

//        You can call flush if you want to but if you call close it will always flush

//        Producer close will also flush all of your data as well
        producer.close();
    }
}