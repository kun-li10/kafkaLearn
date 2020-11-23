package com.lk.kafka.serialize;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.stream.IntStream;

/**
 * 使用自定义序列化进行网络传输value
 *
 * @author lk
 * @version 1.0
 * @date 2020/11/22 19:22
 */
public class KafkaProducerSerializer {

  public static void main(String[] args) {
    //创建链接
    Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "centos:9092");
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        ObjectDeserializer.class.getName());

    //创建生产者
    KafkaProducer<String, User> producer = new KafkaProducer<>(properties);

    //封装消息队列
    IntStream.range(0, 10).forEach(i -> {
      ProducerRecord<String, User> record = new ProducerRecord<>("topic02", "key" + i,
          new User("name" + i, "address" + i, i));
      producer.send(record);
    });

    //关闭
    producer.close();
  }
}
