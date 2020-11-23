package com.lk.kafka.service;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * 集成Kafka-boot
 *
 * @author lk
 * @version 1.0
 * @date 2020/11/22 21:57
 */

@Service
@Transactional
public class OrderServiceImpl implements OrderService {

  @Autowired
  private KafkaTemplate kafkaTemplate;

  @Override
  public void saveOrder(String key, String message) {
    kafkaTemplate.send(new ProducerRecord("topic04", key, message));
  }
}
