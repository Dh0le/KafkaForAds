package com.imooc.kafkastudy;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.network.Send;

import java.util.Properties;

public class MyProducer {
    static{
        Properties properties = new Properties();
        properties.put("bootstrap.servers","127.0.0.1:9092");
        properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("partitioner.class","com.imooc.kafkastudy.CustomPartitioner");
        producer = new KafkaProducer<>(properties);
    }


    private static class MyProducerCallback implements Callback{

        @Override
        public void onCompletion(RecordMetadata result, Exception e) {
            if(e != null){
                e.printStackTrace();
                return;
            }
            System.out.println(result.topic());
            System.out.println(result.partition());
            System.out.println(result.offset());
            System.out.println("Coming in MyProducerCallback");
        }
    }
    private static void sendMessageCallback(){
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("imooc-kafka-study",
                "name",
                "callback");

        ProducerRecord<String, String> record1 = new ProducerRecord<String, String>("imooc-kafka-study",
                "name-x",
                "callback");
        ProducerRecord<String, String> record2 = new ProducerRecord<String, String>("imooc-kafka-study",
                "name-y",
                "callback");
        producer.send(record,new MyProducerCallback());
        producer.send(record1,new MyProducerCallback());
        producer.send(record2,new MyProducerCallback());
        producer.close();
    }

    public static void main(String[] args) throws Exception{
        //sendMessageForgetResult();
        //sendMessageSync();
        sendMessageCallback();
    }

    private static void sendMessageSync() throws Exception {
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("imooc-kafka-study",
                "name",
                "sync");
        RecordMetadata result = producer.send(record).get();
        System.out.println(result.topic());
        System.out.println(result.partition());
        System.out.println(result.offset());
        producer.close();
    }

    private static void sendMessageForgetResult() {
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("imooc-kafka-study",
                "name",
                "ForgetResult");
        producer.send(record);
        producer.close();
    }

    private static KafkaProducer<String, String> producer;
}
