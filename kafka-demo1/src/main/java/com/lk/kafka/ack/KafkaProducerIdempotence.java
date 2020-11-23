package com.lk.kafka.ack;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.stream.IntStream;

/**
 * Kafka 0.11.0.0版本以后增加对幂等的支持
 * 生产者 幂等写
 * 由于生产者ack确认及reties重试,ack确认时间 可能会导致ack确认超时造成重试，
 * 从而引起生产者重复发送数据
 * <p>
 * 生产者Reties: 保证数据的不丢失 默认reties=Integer.MAX_VALUE
 * 幂等性: 保证数据不会重复提交
 * <p>
 * 注意: 使用幂等性,要求必须开启retries=true 和acks=all(-1)
 *
 * @author lk
 * @version 1.0
 * @date 2020/11/22 19:22
 */
public class KafkaProducerIdempotence {

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
    //开启幂等写
    properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
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
