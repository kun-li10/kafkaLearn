package com.lk.kafka.producer_consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Iterator;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * 消费者
 *
 * @author lk
 * @version 1.0
 * @date 2020/11/22 19:36
 */
public class KafkaConsumerDemo {

  static final Pattern compile = Pattern.compile("^topic.*$");

  public static void main(String[] args) {
    //1.参数配置
    Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "centos:9092");
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer.class.getName());
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer.class.getName());
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");

    //2.创建消费者
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

    //3.订阅topic
    consumer.subscribe(compile);

    while (true) {
      ConsumerRecords<String, String> consumerRecords =
          consumer.poll(Duration.ofSeconds(1).getSeconds());
      Iterator<ConsumerRecord<String, String>> iterator = consumerRecords.iterator();
      while (iterator.hasNext()) {
        ConsumerRecord<String, String> consumerRecord = iterator.next();
        String key = consumerRecord.key();
        long offset = consumerRecord.offset();
        String value = consumerRecord.value();
        int partition = consumerRecord.partition();
        System.out.printf("key: %s,offset: %s,value:%s,pritition:%s", key, offset, value,
            partition);
      }
    }

  }
}
