package com.lk.kafka.offset;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * 消费者,不开启自动提交offset
 * 消费后手动提交,手动管理提交offset
 * 注意: 用户提交的offset偏移量永远都要比本次消费的偏移量+1.
 * 提交的offset是kafka下一次抓取数据的位置
 *
 * @author lk
 * @version 1.0
 * @date 2020/11/22 19:36
 */
public class KafkaConsumerDefineOffset {

  static final Pattern compile = Pattern.compile("^topic.*$");

  public static void main(String[] args) {
    //1.参数配置
    Properties properties = new Properties();
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer.class.getName());
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "centos:9092");
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer.class.getName());
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");
    //关闭consumer自动提交offset
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

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
        int partition = consumerRecord.partition();
        long offset = consumerRecord.offset();
        String value = consumerRecord.value();
        String key = consumerRecord.key();
        //consumer手动管理offset
        Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataHashMap =
            new HashMap<>();

        //注意offset必须加1,不然会造成消费不完的情况
        topicPartitionOffsetAndMetadataHashMap.put(new TopicPartition(consumerRecord.topic(),
            partition), new OffsetAndMetadata(offset + 1));
        consumer.commitAsync(topicPartitionOffsetAndMetadataHashMap, new OffsetCommitCallback() {
          @Override
          public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets,
                                 Exception exception) {
            System.out.println("final commit offset :" + offset + ",exception :" + exception);
          }
        });
        System.out.printf("key: %s,offset: %s,value:%s,pritition:%s", key, offset, value,
            partition);

      }
    }

  }
}
