/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.kafka.twitter;

import com.google.common.collect.Lists;
import com.mycompany.kafka.ConsumerDemoGroups;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author sendakil
 */
public class TwitterProducer {
    
    String consumerkey="GGqICzo4vqELlFri3QRxMEQVJ";
    String consumersecret="AF3nECcBHyHuEe2CgCkQPWhHDXe9dYY0CRu51u9gUDY5087Lhg";
    String token="244175665-DsnldAmpjVC3OddyGp8FQbY3A95ffHxz722g9iqU";
    String secret="3ekCpiOWCBic1wbv6SeeHmXH6k84z9dB4E6T9Xl8gfPbk"; 
    List<String> terms = Lists.newArrayList("#kafkatest");
    Logger logger= LoggerFactory.getLogger(ConsumerDemoGroups.class.getName());
    
    
    public TwitterProducer(){}
    
    public static void main(String[] args) throws InterruptedException {
        new TwitterProducer().run();
        
        
    }
    
    public void run() throws InterruptedException{
        
       logger.info("Setup");
       /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
       BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1000);
        
        // create a twitter client
        Client client= createTwitterClient(msgQueue);
        client.connect();
        
        // create a kafka producer
        KafkaProducer<String, String> producer = createKafkaProducer();
        
        // loop to send tweets to kafka
        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
          String msg=null; 
          try{
              msg = msgQueue.poll(5,TimeUnit.SECONDS);
          } catch (InterruptedException e){
              e.printStackTrace();
              client.stop();
          }
          if (msg != null){
            logger.info(msg);
            producer.send(new ProducerRecord<>("twitter_tweets",null,msg), new Callback(){
                public void onCompletion(RecordMetadata recordMetadata, Exception e){
                    if (e !=null){
                        logger.error("Something bad happened",e);
                    }
                }
            }
            
            );
          }
          
        }
       logger.info("End of Application");
    }
    
   
    
    public Client createTwitterClient(BlockingQueue<String> msgQueue){
   
       /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        
        // Optional: set up some followings and track terms
     
       
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerkey, consumersecret, token, secret);
        
        ClientBuilder builder = new ClientBuilder()
          .name("Hosebird-Client-01")                              // optional: mainly for the logs
          .hosts(hosebirdHosts)
          .authentication(hosebirdAuth)
          .endpoint(hosebirdEndpoint)
          .processor(new StringDelimitedProcessor(msgQueue));
   

        Client hosebirdClient = builder.build();
        return hosebirdClient;

    }
    
    public KafkaProducer<String, String> createKafkaProducer(){
          Properties properties = new Properties();
        String bootstrapservers="127.0.0.1:9092";
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapservers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        // create the producer
        KafkaProducer<String, String> producer=new  KafkaProducer<String, String>(properties);
        return producer;
    }
}
