package com.lk.kafka.kafka_native;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Properties;
import java.util.regex.Pattern;


/**
 * @author lk
 * @version 1.0
 * @date 2020/6/13 18:40
 */
public class KafkaConsumerDemo extends Thread {

  private final KafkaConsumer<String, String> consumer;

  public KafkaConsumerDemo(String topic) {
    Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "111.231.107.218:9092");
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaConsumerDemo");
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer.class.getName());
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer.class.getName());
    consumer = new KafkaConsumer<String, String>(properties);
//        consumer.subscribe(Collections.singletonList(topic));
    //指定消息消费固定的分区
    TopicPartition partition = new TopicPartition(topic, 0);
//    consumer.assign(Arrays.asList(partition));

    //不指定分区,使用消费组的特性,消息分发的策略等
    consumer.subscribe(Pattern.compile("^topic.*"));
  }

  @Override
  public void run() {
    while (true) {
      ConsumerRecords<String, String> consumerRecords =
          consumer.poll(Duration.ofSeconds(1).getSeconds());
      for (ConsumerRecord record : consumerRecords) {
        System.out.println("partition: " + record.partition() + "acceptMeg: " + record.value());
      }
    }
  }

  public static void main(String[] args) {
    new KafkaConsumerDemo("topic01").start();
  }
}
