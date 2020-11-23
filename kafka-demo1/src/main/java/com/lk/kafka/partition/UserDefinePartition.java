package com.lk.kafka.partition;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.utils.Utils;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 自定义分区 轮询
 *
 * @author lk
 * @version 1.0
 * @date 2020/11/22 19:50
 */
public class UserDefinePartition implements Partitioner {

  AtomicInteger atomicInteger = new AtomicInteger();

  @Override
  public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes
      , Cluster cluster) {
    //所有分区
    int size = cluster.availablePartitionsForTopic(topic).size();
    if (keyBytes == null || keyBytes.length == 0) {
      return atomicInteger.addAndGet(1) & Integer.MAX_VALUE % size;
    }
    return Utils.toPositive(Utils.murmur2(keyBytes)) % size;
  }

  @Override
  public void close() {
    System.out.println("close");
  }

  @Override
  public void configure(Map<String, ?> configs) {
    System.out.println("configure");
  }
}
