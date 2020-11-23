package com.lk.kafka.transactional;

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
 * 当Producer生产者开启事务后
 * 消费者必须把隔离级别设置为读已提交
 * isolation.level=read_committed
 *
 * @author lk
 * @version 1.0
 * @date 2020/11/22 19:36
 */
public class KafkaConsumerTransaction {

  static final Pattern compile = Pattern.compile("^topic.*$");

  public static void main(String[] args) {
    //1.参数配置
    Properties properties = new Properties();
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer.class.getName());
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "centos:9092");
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer.class.getName());
   //设置隔离级别 读已提交
    properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG,"read_committed");
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
        long offset = consumerRecord.offset();
        String key = consumerRecord.key();
        String value = consumerRecord.value();
        int partition = consumerRecord.partition();
        System.out.printf("key: %s,offset: %s,value:%s,pritition:%s", key, offset, value,
            partition);
      }
    }

  }
}
