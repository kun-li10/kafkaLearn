package com.lk.kafka.serialize;

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
 * value使用自定义反序列化
 *
 * @author lk
 * @version 1.0
 * @date 2020/11/22 19:36
 */
public class KafkaConsumerSerialize {

  static final Pattern compile = Pattern.compile("^topic.*$");

  public static void main(String[] args) {
    //1.参数配置
    Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "centos:9092");
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer.class.getName());
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        ObjectDeserializer.class.getName());
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group2");

    //2.创建消费者
    KafkaConsumer<String, User> consumer = new KafkaConsumer<>(properties);

    //3.订阅topic
    consumer.subscribe(compile);

    while (true) {
      ConsumerRecords<String, User> consumerRecords =
          consumer.poll(Duration.ofSeconds(1).getSeconds());
      Iterator<ConsumerRecord<String, User>> iterator = consumerRecords.iterator();
      while (iterator.hasNext()) {
        ConsumerRecord<String, User> consumerRecord = iterator.next();
        long offset = consumerRecord.offset();
        String key = consumerRecord.key();
        User value = consumerRecord.value();
        int partition = consumerRecord.partition();
        System.out.printf("key: %s,offset: %s,value:%s,pritition:%s", key, offset, value,
            partition);
      }
    }

  }
}
