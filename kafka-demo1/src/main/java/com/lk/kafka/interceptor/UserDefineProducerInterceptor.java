package com.lk.kafka.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * 自定义生产者拦截器
 *
 * @author lk
 * @version 1.0
 * @date 2020/11/22 20:24
 */
public class UserDefineProducerInterceptor implements ProducerInterceptor {

  @Override
  public ProducerRecord onSend(ProducerRecord record) {
    //拦截后,添加头信息
    ProducerRecord wrapRecord = new ProducerRecord(record.topic(), record.key(),
        record.value());
    wrapRecord.headers().add("user", "likun".getBytes());
    return wrapRecord;
  }

  @Override
  public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
    System.out.println("metadata: " + metadata + ",exception: " + exception);
  }

  @Override
  public void close() {
    System.out.println("ProducerInterceptor close");
  }

  @Override
  public void configure(Map<String, ?> configs) {
    System.out.println("ProducerInterceptor configure");
  }
}
