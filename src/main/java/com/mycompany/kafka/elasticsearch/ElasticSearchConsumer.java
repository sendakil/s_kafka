/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.kafka.elasticsearch;

import com.google.gson.JsonParser;
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

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.logging.Level;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;


/**
 *
 * @author sendakil
 */
public class ElasticSearchConsumer {
    
    public static RestHighLevelClient createClient(){
        
         // replace with your own credentials
         
           String hostname="kafka-course-me-2155227072.ap-southeast-2.bonsaisearch.net";
           String username="vk8pxjsvcm";
           String password="mxrxz5xx1a";
    
      // credentials provider help supply username and password
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient.builder(
                new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });

        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    
        
    }
    
    public static KafkaConsumer<String, String> createConsumer(String topic){
        Properties properties = new Properties();
        String bootstrapservers="127.0.0.1:9092";
        String groupid="kafka-demo-elasticsearch1";
       //String topic = "twitter_tweets";
        
        // create consumer configs
        
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapservers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupid);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
          
        // create consumer
        
        KafkaConsumer<String, String> consumer = new KafkaConsumer<> (properties);
        consumer.subscribe(Arrays.asList(topic));
        return consumer;
    }
 
     public static void main(String[] args) throws IOException{
         
         Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
           RestHighLevelClient client = createClient();
           
                    
            KafkaConsumer<String, String> consumer=createConsumer("twitter_tweets");
            
                // poll for new data
        
            while(true){
                ConsumerRecords<String, String> records=consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String,String> record: records){
                    // 2 Strategies 
                    // kafka_generic id
                    //String id=record.topic()+"_"+record.partition()+"_"+record.offset();
                    
                    // twitter feed specific id
                    
                    String id=extractIdFromTweet(record.value());
                    
                    // where we insert data into Elasticsearch
                     IndexRequest indexrequest= new IndexRequest("twitter","tweets",id)
                    .source(record.value(),XContentType.JSON);
             
                    
                     IndexResponse indexresponse=client.index(indexrequest, RequestOptions.DEFAULT);
                     logger.info(indexresponse.getId());
                   
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                                   
            // close the client gracefully
           //  client.close();
        }
         
     }
     
     private static JsonParser jsonParser = new JsonParser();
     
     private static String extractIdFromTweet(String tweetjson){
            return jsonParser.parse(tweetjson)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();
     }
     
}
