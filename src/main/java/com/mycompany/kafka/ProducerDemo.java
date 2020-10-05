/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.kafka;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 *
 * @author sendakil
 */
public class ProducerDemo {
    public static void main(String[] args) {
        // create producer properties
        Properties properties = new Properties();
        String bootstrapservers="127.0.0.1:9092";
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapservers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        // create the producer
        KafkaProducer<String, String> producer=new  KafkaProducer<String,String>(properties);
        // create a producer record
        ProducerRecord<String, String> record = new ProducerRecord<String, String> ("first_topic","hello world1");
        // send data - asynchronus
        producer.send(record);
        // flush data 
        producer.flush();
        // flush close
        producer.close();
        
    }
    
}
