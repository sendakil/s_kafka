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
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author sendakil
 */
public class ConsumerDemo {
    
    public static void main(String[] args) {
        Logger logger= LoggerFactory.getLogger(ConsumerDemo.class.getName());
        
        Properties properties = new Properties();
        String bootstrapservers="127.0.0.1:9092";
        String groupid="my-fourth-application";
        String topic = "first_topic";
        
        // create consumer configs
        
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapservers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupid);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
          
        // create consumer
        
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String> (properties);
        
        // subscribe consumer to our topic
        
        consumer.subscribe(Arrays.asList(topic));
        
        // poll for new data
        
        while(true){
            ConsumerRecords<String, String> records=consumer.poll(Duration.ofMillis(100));
            
            for (ConsumerRecord<String,String> record: records){
                logger.info("Key: "+ record.key()+ ", Value:" + record.value());
                logger.info("Partition: "+ record.partition() + ", Offset:",record.offset() );
                
            }
            
        }
        
        
        
    }
            
            
            
    
}
