/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.kafka;


import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Callback;
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
public class ProducerDemoKeys {
    public static void main(String[] args) throws InterruptedException, ExecutionException {
        // create producer properties
        
        final Logger logger=  LoggerFactory.getLogger(ProducerDemoKeys.class);
        
        Properties properties = new Properties();
        String bootstrapservers="127.0.0.1:9092";
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapservers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        // create the producer
        KafkaProducer<String, String> producer=new  KafkaProducer<>(properties);
        
        for (int i=0;i<10;i++){
            
        String topic="first_topic";
        String value="Hello World"+Integer.toString(i);
        String key="id_" + Integer.toString(i);
        logger.info("Key: " + key);
        
              // create a producer record
        ProducerRecord<String, String> record = new ProducerRecord<> (topic,key,value);
        // send data - asynchronus
        producer.send(record, new Callback(){

           public void onCompletion(RecordMetadata recordMetadata, Exception e) {
              // executes every time a record is successfully sent or exeption is thrown
              if (e == null) {
                  logger.info("Received new metadata \n" + 
                          "Topic:"  + recordMetadata.topic() +"\n" +
                          "Partition:"  + recordMetadata.partition() +"\n" +
                          "Offset:" + recordMetadata.offset() +
                          "Timestamp:" + recordMetadata.timestamp()
                                    );
                  // the record was successfully sent
              } else {
                    logger.error("Error while producing",e);
              }
          }
         
        
        }).get();
            
        }
      
        // flush data 
        producer.flush();
        // flush close
        producer.close();
        
    }
    
}
