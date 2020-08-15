package com.github.rkaahean.kafkatwitterconsumer;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
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
import java.util.Properties;

public class ElasticSearchConsumer {

    public static void main(String[] args) throws IOException {

        // Creating a Logger
        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());


        RestHighLevelClient client = createClient();

        String jsonString = "{ \"foo\": \"bar\"}";

        // Need to pass JSON. Make sure "twitter" index is already created in ElasticSearch via
        // console
        IndexRequest indexRequest = new IndexRequest(
                "twitter",
                "tweets"
        ).source(jsonString, XContentType.JSON);

        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        String id = indexResponse.getId();
        logger.info(id);

        // Finally closing client gracefully
        client.close();
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
}
