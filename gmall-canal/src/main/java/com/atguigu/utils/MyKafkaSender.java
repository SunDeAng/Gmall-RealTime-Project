package com.atguigu.utils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @Author: Sdaer
 * @Date: 2020-08-18
 * @Desc: 发送数据到Kafka
 */
public class MyKafkaSender {

    //声明Kafka生产者
    public static KafkaProducer<String,String> kafkaProducer = null;

    //创建Kafka生产者的方法
    public static KafkaProducer<String,String> createKafkaProducer(){
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String,String> producer = null;
        producer = new KafkaProducer<>(properties);
        return producer;
    }

    //发送数据的方法
    public static void send(String topic,String msg){
        if (kafkaProducer == null){
            kafkaProducer = createKafkaProducer();
        }
        kafkaProducer.send(new ProducerRecord<>(topic,msg));
    }

}
