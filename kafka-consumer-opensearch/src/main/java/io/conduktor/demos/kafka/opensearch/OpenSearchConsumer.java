package io.conduktor.demos.kafka.opensearch;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OpenSearchConsumer {


    public static RestHighLevelClient createOpenSearchClient(){
//        Connection String for Localhost and Docker
//        String connString = "http://localhost:9092";

//        Bonsai Connection String
        String connString = "https://t4xhf7sebf:k3qc8p6q8m@kafka-course-4974295163.us-west-2.bonsaisearch.net:443";

        // we build a URI from the connection string
        RestHighLevelClient restHighLevelClient;
        URI connUri = URI.create(connString);
        // extract login information if it exists
        String userInfo = connUri.getUserInfo();

        if (userInfo == null) {
            // REST client without security
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));

        } else {
            // REST client with security
            String[] auth = userInfo.split(":");

            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));


        }

        return restHighLevelClient;
    }

    private static KafkaConsumer<String,String> createKafkaConsumer(){

        String groupId = "consumer-opensearch-demo";

        String bootstrapServers = "127.0.0.1:9092";

        Properties properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
//        Create a Consumer
       return new KafkaConsumer<>(properties);
    }

    public static void main(String[] args) throws IOException {

        Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());

        //First create an OpenSearch Client
        RestHighLevelClient openSearchClient = createOpenSearchClient();

        //Create Our Kafka Client
        KafkaConsumer<String, String> consumer = createKafkaConsumer();


        // We need to create the index of OpenSearch if it does not exist

        try(openSearchClient; consumer) {

            boolean indexExists = openSearchClient.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);

            if (!indexExists) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                log.info("The Wikimedia index has been created");
            } else {
                log.info("The Wikimedia index already exists");
            }


            // Sub the consumer
//        consumer.subscribe(Collections.singleton("wikimedia.recentchange"));

            consumer.subscribe(Collections.singleton("wikimedia.recentchange"));
//try{

            while (true) {

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));

                int recordCount = records.count();
                log.info("Received: " + recordCount + " record(s)");

                for (ConsumerRecord<String, String> record : records) {

                    try {
                        //Send the record into OpenSearch
                        IndexRequest indexRequest = new IndexRequest("wikimedia")
                                .source(record.value(), XContentType.JSON);

                        IndexResponse response = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);

                        openSearchClient.index(indexRequest, RequestOptions.DEFAULT);

                        log.info(response.getId(), "This is inserting 1 document into OpenSearch");

                    } catch (Exception e){
                        // Do Nothing For Now
                    }
                }
            }


            //Code Logic


            //Create Our Kafka Client

            //Close Things

        }

    }
}
