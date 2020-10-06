/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author sendakil
 */
public class ConsumerDemoAssignSeek {
    
    public static void main(String[] args) {
        Logger logger= LoggerFactory.getLogger(ConsumerDemoAssignSeek.class.getName());
        
        Properties properties = new Properties();
        String bootstrapservers="127.0.0.1:9092";
        String topic = "first_topic";
        
        // create consumer configs
        
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapservers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
              properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
          
        // create consumer
        
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String> (properties);
        
        // subscribe consumer to our topic
        
        // assign and seek are mostly used to replay data or fetch a specific message
        
        // assign
        TopicPartition partitionToReadFrom= new TopicPartition(topic,0);
        long OffsetToReadFrom=15L;
        consumer.assign(Arrays.asList(partitionToReadFrom));
        
        // seek
        consumer.seek(partitionToReadFrom,OffsetToReadFrom);
        
        int numberOfMessageToRead=5;
        boolean keepOnReading=true;
        int numberOfMessagesReadSofar=0;
        
        
        // poll for new data
        
        while(keepOnReading){
            ConsumerRecords<String, String> records=consumer.poll(Duration.ofMillis(100));
            
            for (ConsumerRecord<String,String> record: records){
                numberOfMessagesReadSofar +=1;
                logger.info("Key: "+ record.key()+ ", Value:" + record.value());
                logger.info("Partition: "+ record.partition() + ", Offset:",record.offset() );
                if (numberOfMessagesReadSofar >=numberOfMessageToRead){
                    keepOnReading=false;
                    break;
                }
            }
            
        }
        
        logger.info("Exiting the application ");   
        
    }
            
            
  
    
}
