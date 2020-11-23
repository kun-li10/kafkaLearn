package com.lk.kafka.offset;

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
 * 消费者,开启自动提交offset
 * 默认是开启自动提交  间隔时间:5s
 * 所以可能会存在Consumer5s内存在故障,导致offset提交失败,造成重复消费
 *
 * @author lk
 * @version 1.0
 * @date 2020/11/22 19:36
 */
public class KafkaConsumerEnableOffset {

  static final Pattern compile = Pattern.compile("^topic.*$");

  public static void main(String[] args) {
    //1.参数配置
    Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "centos:9092");
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer.class.getName());
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer.class.getName());
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");
    //默认自动提交offset 5s  间隔时间
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
    properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 100000);

    //当consumer第一次启动时没有offset时,策略消费 默认latest 最新记录开始
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

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
        String value = consumerRecord.value();
        int partition = consumerRecord.partition();
        String key = consumerRecord.key();
        System.out.printf("key: %s,offset: %s,value:%s,pritition:%s", key, offset, value,
            partition);
      }
    }

  }
}
