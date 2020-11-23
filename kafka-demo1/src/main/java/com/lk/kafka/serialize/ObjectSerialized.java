package com.lk.kafka.serialize;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.common.serialization.Serializer;

import java.io.Serializable;
import java.util.Map;

/**
 * 自定义序列化
 * @author lk
 * @version 1.0
 * @date 2020/11/22 20:06
 */
public class ObjectSerialized implements Serializer<Object> {

  @Override
  public byte[] serialize(String topic, Object data) {
    return SerializationUtils.serialize((Serializable) data);
  }

  @Override
  public void close() {
    System.out.println("serialized close");
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    System.out.println("serialized config");
  }
}
