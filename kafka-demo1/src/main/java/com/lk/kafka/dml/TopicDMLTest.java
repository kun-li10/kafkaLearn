package com.lk.kafka.dml;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * 测试Kafka的基本操作
 *
 * @author lk
 * @version 1.0
 * @date 2020/11/22 13:00
 */
public class TopicDMLTest {

  public static void main(String[] args) throws ExecutionException, InterruptedException {
    Properties properties = new Properties();
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "111.231.107.218:9092");
    KafkaAdminClient client = (KafkaAdminClient) KafkaAdminClient.create(properties);
    //查询topic
    KafkaFuture<Set<String>> future = client.listTopics().names();
    for (String name : future.get()) {
      System.out.println(name + "\t");
    }

    //创建topic
    List<NewTopic> newTopics = Arrays.asList(new NewTopic("topic02", 2, (short) 3));
    CreateTopicsResult topics = client.createTopics(newTopics);
    System.out.println("Create Topic: " + topics.all().get());

    //删除topic
    DeleteTopicsResult topic02 = client.deleteTopics(Arrays.asList("topic02"));

    //查看topic详情
    DeleteTopicsResult deleteTopicsResult = client.deleteTopics(Arrays.asList("topic02"));
    Map<String, KafkaFuture<Void>> values = deleteTopicsResult.values();
    for (String topicName : values.keySet()) {
      System.out.println("topic: " + topicName);
    }
    //关闭
    client.close();
  }
}
