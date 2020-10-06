/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.kafka.elasticsearch;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;
import org.elasticsearch.client.RestHighLevelClient;

/**
 *
 * @author sendakil
 */
public class ElasticSearchConsumer {
    
    public static RestHighLevelClient createClient(){
           String hostname="";
           String username="";
           String password="";
    
    
     final CredentialsProvider credentialsProvider =   new BasicCredentialsProvider();
     credentialsProvider.setCredentials(AuthScope.ANY,  new UsernamePasswordCredentials("user", "password"));

     RestClientBuilder builder = RestClient.builder(  new HttpHost(hostname,443, "https"))
             .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
        @Override
        public HttpAsyncClientBuilder customizeHttpClient(
                HttpAsyncClientBuilder httpClientBuilder) {
                   return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
        }
     });
     RestHighLevelClient client=new RestHighLevelClient(builder);
     return client;
     
        
    }
 
        
     public static void main(String[] args){
         System.out.println("Test");
     }
     
}
