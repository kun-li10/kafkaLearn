package com.lk.kafka.transactional;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.stream.IntStream;

/**
 * Kafka 0.11.0.0版本中支持事务操作
 * kafka事务通常分为:  生产者事务only  /  消费者&生产者事务
 * 消费者消费级别隔离级别默认是
 * isolation.level = read_uncommitted 默认
 * 如果开启事务后,消费端必须将事务隔离级别设置为
 * isolation.level = read_committed
 * <p>
 * 生产者开启事务时,默认开启幂等写
 *
 * @author lk
 * @version 1.0
 * @date 2020/11/22 19:22
 */
public class KafkaProducerTransactionOnly {

  public static void main(String[] args) {
    //创建链接
    Properties properties = new Properties();
    //取值必须是唯一的,同一时刻只能有一个transaction-id存在
    properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transaction-id");

    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "centos:9092");
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    //创建生产者
    KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
    //初始化事务
    producer.initTransactions();
    try {
      //开启事务
      producer.beginTransaction();
      IntStream.range(0, 4).forEach(i -> {
        //封装消息队列
        ProducerRecord<String, String> record = new ProducerRecord<>("topic02", "key" + i,
            "value" + i);
        producer.send(record);
      });
      //提交事务
      producer.commitTransaction();
    } catch (Exception e) {
      System.out.println("生产者发送Fail: " + e.getMessage());
      //终止事务
      producer.abortTransaction();
    }

    //关闭
    producer.close();
  }
}
