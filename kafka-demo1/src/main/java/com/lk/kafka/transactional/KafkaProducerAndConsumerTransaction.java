package com.lk.kafka.transactional;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

/**
 * kafka事务另外一种,生产者&消费者事务
 * 微服务框架中,其中的一个模块可能充当
 * 生产者和消费者
 * 消费后继续扔到下一个Topic中，供下一个service消费
 *
 * @author lk
 * @version 1.0
 * @date 2020/11/22 21:34
 */
public class KafkaProducerAndConsumerTransaction {

  private static final String CONSUMER_GROUP = "group";

  public static void main(String[] args) {

    //1.生产者 & 消费者
    KafkaProducer<String, String> producer = buildKafkaProducer();
    KafkaConsumer<String, String> consumer = buildKafkaConsumer(CONSUMER_GROUP);

    consumer.subscribe(Arrays.asList("topic1"));
    //初始化事务
    producer.initTransactions();

    try {

      while (true) {
        ConsumerRecords<String, String> consumerRecords =
            consumer.poll(Duration.ofSeconds(1).getSeconds());
        //封装提交的offset
        Map<TopicPartition, OffsetAndMetadata> offsetAndMetadataHashMap =
            new HashMap<>();
        Iterator<ConsumerRecord<String, String>> iterator = consumerRecords.iterator();
        //开启事务
        producer.beginTransaction();
        while (iterator.hasNext()) {
          ConsumerRecord<String, String> consumerRecord = iterator.next();
          //创建Record
          ProducerRecord<String, String> producerRecord = new ProducerRecord<>("topic02",
              consumerRecord.key(), consumerRecord.value() + "处理完成,继续下一个Topic!");
          producer.send(producerRecord);
          //记录元数据
          offsetAndMetadataHashMap.put(new TopicPartition(consumerRecord.topic(),
              consumerRecord.partition()), new OffsetAndMetadata(consumerRecord.offset() + 1));
          //提交事务
          producer.sendOffsetsToTransaction(offsetAndMetadataHashMap, CONSUMER_GROUP);
          producer.commitTransaction();
        }
      }

    } catch (Exception e) {
      System.out.println("KafkaProducerAndConsumerTransaction Fail: " + e.getMessage());
      //终止事务
      producer.abortTransaction();
    }
  }


  /**
   * Create 生产者
   *
   * @return
   */
  public static KafkaProducer<String, String> buildKafkaProducer() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "CentOSA:9092,CentOSB:9092,CentOSC:9092");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transaction-id");
    return new KafkaProducer<String, String>(props);
  }


  /**
   * Create Consumer
   *
   * @param group
   * @return
   */
  public static KafkaConsumer<String, String> buildKafkaConsumer(String group) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "CentOSA:9092,CentOSB:9092,CentOSC:9092");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

    return new KafkaConsumer<String, String>(props);
  }

}
