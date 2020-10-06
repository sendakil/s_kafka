/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.kafka;

import java.time.Duration;
import java.util.Arrays;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author sendakil
 */
public class ConsumerDemoWithThread {
    
    public static void main(String[] args) {
       new ConsumerDemoWithThread().run();
    }
      
        
        // poll for new data
        
      private ConsumerDemoWithThread(){
          
      }
      
      private void run(){
        Logger logger= LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());

        String bootstrapservers="127.0.0.1:9092";
        String groupid="my-fifth-application";
        String topic = "first_topic";
        
        // latch for dealing with multiple threads
        
        CountDownLatch latch= new CountDownLatch(1);
        
              
       // create the consumer runnable 
        logger.info("Creating consumer thread");
        
        Runnable myConsumerRunnable = new ConsumerRunnable(
                    bootstrapservers,
                    groupid,
                    topic,
                    latch
        ); 
         
        // start the thread 
       
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();
        
        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook( new Thread( () -> {
            logger.info("Caught Shutdown hook");
            ((ConsumerRunnable) myConsumerRunnable).shutdown();
            try {
                latch.await();
               } catch (InterruptedException e){
                   e.printStackTrace();
               }
               logger.info("Application has exited");
        }       
        ));
        
        
        
        try { 
            latch.await();
        } catch (InterruptedException e){
            e.printStackTrace();
            logger.error("Application got interrupted",e);
        } finally {
            logger.info("Application is closing");
        }
        
        
      }
        
       
        public class ConsumerRunnable implements Runnable {
           private CountDownLatch latch;
           private KafkaConsumer<String, String> consumer;
           private  Logger logger= LoggerFactory.getLogger(ConsumerRunnable.class.getName());
           
           public ConsumerRunnable(
                   String bootstrapservers,
                   String groupid,
                   String topic,
                   CountDownLatch latch){
               this.latch=latch;
               
                // create consumer configs
                Properties properties = new Properties();
                properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapservers);
                properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
                properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
                properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupid);
                properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
               
                 // create consumer
        
                   consumer = new KafkaConsumer<String, String> (properties);
        
                  // subscribe consumer to our topic
        
                   consumer.subscribe(Arrays.asList(topic));
               
           } 
           @Override
           public void run(){
               // poll for new data
               try { 
                    while(true){
                    ConsumerRecords<String, String> records=consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String,String> record: records){
                        logger.info("Key: "+ record.key()+ ", Value:" + record.value());
                        logger.info("Partition: "+ record.partition() + ", Offset:",record.offset() );

                    }
                    } 
               }   catch (WakeupException e ){
                   logger.info("Received shutdown signal!");
               }  finally {
                   consumer.close();
                   // tell our main code we are done with consumer
                   latch.countDown();
               }
               
        }
               
          
           public void shutdown(){
            // the wakeup() method is a special method to interrupt consumer.poll() 
            // it will throw the exception WakeupException
            consumer.wakeup();
           }
            
        }
        
        
   
            
            
            
    
}
