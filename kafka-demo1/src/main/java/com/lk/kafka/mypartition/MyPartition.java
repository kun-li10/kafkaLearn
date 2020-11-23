package com.lk.kafka.mypartition;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * 自定义消息发送的分区
 * 默认: hash取模
 * @author lk
 * @version 1.0
 * @date 2020/6/14 10:42
 */
public class MyPartition implements Partitioner {

    public Random random = new Random();

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        //获取所有分区
        List<PartitionInfo> partitionInfos = cluster.availablePartitionsForTopic(topic);
        int partitionNum;
        if (key == null) {
            //随机分配分区
            partitionNum = random.nextInt(partitionInfos.size());
        } else {
            partitionNum = Math.abs((key.hashCode()) % partitionInfos.size());
        }
        //指定发送分区
        return partitionNum;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
