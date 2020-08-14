package com.github.rkaahean.kafkatwitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    List<String> terms = Lists.newArrayList("sancho", "barca", "bayern");

    public TwitterProducer(){



    }

    public static void main(String[] args) {

            // Running the main Twitter Producer
            new TwitterProducer().run();
    }

    public void run(){

        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        // This is where all the messages are stored/
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1000);
        String bootstrapServers = "64.225.36.250:9092";

        // create a twitter client based on configurations.
        Client client = createTwitterClient(msgQueue);
        client.connect();

        // create a kafka producer. Pass in Server IP with port
        KafkaProducer<String, String> kafkaProducer = createKafkaProducer(bootstrapServers);

        // adding a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            // stopping application
            System.out.println("Stopping application.");

            client.stop();
            kafkaProducer.close();


        }));

        // loop to send tweets
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }

            if (msg != null){
                //System.out.println(msg);
                kafkaProducer.send(new ProducerRecord<>("twitter_tweets", null, msg), new Callback(){
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e){
                        if(e != null){
                            System.out.println("Something bad happened");
                        }
                    }
                });
            }
        }
    }

    public Properties getTwitterCredentials(String location){
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

    public Client createTwitterClient(BlockingQueue<String> msgQueue){

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        // Tracking terms
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Properties credentials = getTwitterCredentials("app.config");

        Authentication hosebirdAuth = new OAuth1(
                credentials.getProperty("api_key"),
                credentials.getProperty("api_secret_key"),
                credentials.getProperty("access_token"),
                credentials.getProperty("access_token_secret")
        );

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build();
    }

    public KafkaProducer<String, String> createKafkaProducer(String bootstrapServers){

        // Create Producer Properties
        Properties properties = new Properties();

        // Adding properties
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Creating a safe producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        // Creating a high throughput producer
        // Adding compression and batching
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20"); // 20 ms
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024)); // 32KB batch size

        // Create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        return producer;

    }
}
