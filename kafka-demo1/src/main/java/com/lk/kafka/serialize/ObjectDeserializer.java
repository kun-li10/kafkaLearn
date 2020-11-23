package com.lk.kafka.serialize;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * 自定义反序列化
 *
 * @author lk
 * @version 1.0
 * @date 2020/11/22 20:09
 */
public class ObjectDeserializer implements Deserializer<Object> {

  @Override
  public Object deserialize(String topic, byte[] data) {
    return SerializationUtils.deserialize(data);
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    System.out.println("Deserializer configer");
  }

  @Override
  public void close() {
    System.out.println("Deserliazer close");
  }
}
