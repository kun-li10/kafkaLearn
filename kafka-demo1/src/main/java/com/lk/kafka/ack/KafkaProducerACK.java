package com.lk.kafka.ack;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.stream.IntStream;

/**
 * Producer 生产者发送消息的ack确认
 * ack : -1: leader等待全套同步副本确认记录,保证至少只要一个同步副本仍处于活动状态,记录就不会丢
 *       1: leader会将Record写到本地日志中,但不会等待所有的follower完全确认情况下做出响应
 *       0: 生产者不会等到服务器的任何确认
 *  request.time.ms:3000 默认ack超时时间
 *  reties: 2147483647默认重试
 *
 *  可能会造成数据的重复发送
 * @author lk
 * @version 1.0
 * @date 2020/11/22 19:22
 */
public class KafkaProducerACK {

  public static void main(String[] args) {
    //创建链接
    Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "centos:9092");
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    //开启ack =all 重试5
    properties.put(ProducerConfig.ACKS_CONFIG, "-1");
    properties.put(ProducerConfig.RETRIES_CONFIG, 5);
    //ack确认超时时间
    properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1);
    //创建生产者
    KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

    //封装消息队列
    IntStream.range(0, 3).forEach(i -> {
      ProducerRecord<String, String> record = new ProducerRecord<>("topic02", "key" + i,
          "value" + i);
      producer.send(record);
    });

    //关闭
    producer.close();
  }
}
