package com.github.rkaahean.kafkatwitterconsumer;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {

    public static void main(String[] args) throws IOException {

        // Creating a Logger
        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

        // Creating the client that interacts with ElasticSearch
        RestHighLevelClient client = createClient();

        // Creating the Kafka Consumer
        KafkaConsumer<String, String> consumer = createConsumer("twitter_tweets");

        while(true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); // new in Kafka 2.0.0

            Integer recordCount = records.count();
            logger.info("Received: " + recordCount + " records.");

            BulkRequest bulkRequest = new BulkRequest();

            for (ConsumerRecord<String, String> record : records){

                // The value to be inserted into elasticSearch
                String jsonString = record.value();

                // Insert to elasticsearch here
                // Need to pass JSON. Make sure "twitter" index is already created in ElasticSearch via
                // console

                // In order to make consumer idempotent, 2 strategies.

                // Kafka generic. In case you cannot find an ID
                // String id = record.topic() + "_" + record.partition() + "_" + record.offset();

                // Another strategy. Something that is specific to the twitter stream
                // The id of the tweet!
                try {
                    String id = extractIdFromTweet(record.value());

                    IndexRequest indexRequest = new IndexRequest(
                            "twitter"
                    ).source(jsonString, XContentType.JSON).id(id);

                    // To add for bulk requesting
                    bulkRequest.add(indexRequest);
                } catch (NullPointerException e){
                    // Occurs mostly because tweet does not have a good structure.
                    // No id_str
                    logger.warn("Skipping bad data: " + record.value());
                }

            }

            // Only if we have non empty records
            if (recordCount > 0) {
                // Creating a Bulk Response
                BulkResponse bulkItemResponses = client.bulk(bulkRequest, RequestOptions.DEFAULT);

                // Committing manually
                logger.info("Committing the offsets...");
                consumer.commitSync();
                logger.info("Offsets committed.");

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        // Finally closing client gracefully
        // client.close();
    }

    public static Properties getCredentials(String location){

        Properties credentials = new Properties();

        InputStream is = null;
        try {
            is = new FileInputStream(location);
        } catch (FileNotFoundException ex) {
            ex.printStackTrace();
        }
        try {
            credentials.load(is);
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        return credentials;
    }

    public static RestHighLevelClient createClient(){

        Properties properties = getCredentials("app.config");

        // Store in app.config file
        String hostname = properties.getProperty("bonsai_host");
        String username = properties.getProperty("bonsai_user");
        String password = properties.getProperty("bonsai_password");


        // Authentication specific to bonsai
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient.builder(
                new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback(
                        new RestClientBuilder.HttpClientConfigCallback() {
                         @Override
                         public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                             return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                         }
                     }
                );

        return new RestHighLevelClient(builder);
    }

    public static KafkaConsumer<String, String> createConsumer(String topic){

        String bootstrapServers = "64.225.36.250:9092";
        String groupId = "kafka-elasticsearch";

        Properties properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // disable auto-commit of records
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");

        // create consumer
        KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<String, String>(properties);

        // subscribing to the consumer
        kafkaConsumer.subscribe(Arrays.asList(topic));
        return kafkaConsumer;
    }

    private static JsonParser jsonParser = new JsonParser();

    private static String extractIdFromTweet(String tweetJson){
        // gson
        return jsonParser.parse(tweetJson)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();
    }
}
