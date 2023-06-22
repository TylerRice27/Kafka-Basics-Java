package io.tyler.demos.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class ProducerDemoKeys {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());

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

//        By running a for loop and producing multiple messages in quick succesion it defaults to a stickyPartioner
//        In other words it batches the messages and sends a group to the same partition rather than sending it
//        Round Robin Style

for (int j = 0 ; j <2 ; j++){


//            Create a Producer Record
    for (int i=0; i<10 ; i++){

//                Externalizing my Kafka parameters then injecting them into my ProducerRecord that I am sending
        String topic = "demo_venom";
        String key = "id_" + i;
        String value = "hello world " + i;

//            Create a Producer Record
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);

//        Send Data
        producer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception e) {
//                Executes every time a record successfully sent or an exception is thrown
                if (e == null){
                    log.info("Key:" + key + "| Partition:" + metadata.partition());
                }else{
                    log.error("Error while producing", e);
                }
            }
        });
    }
    log.info("Batch sepereate");
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