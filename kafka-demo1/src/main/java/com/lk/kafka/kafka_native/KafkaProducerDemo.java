package com.lk.kafka.kafka_native;

import com.lk.kafka.mypartition.MyPartition;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author lk
 * @version 1.0
 * @date 2020/6/13 18:40
 */
public class KafkaProducerDemo extends Thread {

  private final KafkaProducer<String, String> producer;
  private final String topic;

  public KafkaProducerDemo(String topic) {
    this.topic = topic;
    Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "111.231.107.218:9092");
    //ACK确认机制
    properties.put(ProducerConfig.ACKS_CONFIG, "-1");
    //开启重试机制
    properties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 3);
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    //使用自定义分区策略
    properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, MyPartition.class.getName());
    producer = new KafkaProducer<String, String>(properties);
  }

  @Override
  public void run() {
    int num = 0;
    while (num < 2) {
      String message = "message" + num;
      System.out.println("begin message:" + message);
      //指定固定的key
      ProducerRecord<String, String> record = new ProducerRecord<>(topic, message);
//            producer.send(new ProducerRecord<Integer, String>(topic, message));
      //回调
      producer.send(record, new Callback() {
        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
          int keySize = metadata.serializedKeySize();
          int valueSize = metadata.serializedValueSize();
          int partition = metadata.partition();
        }
      });
      num++;
    }
  }

  public static void main(String[] args) {
    KafkaProducerDemo test = new KafkaProducerDemo("topic01");
    test.start();
  }
}
