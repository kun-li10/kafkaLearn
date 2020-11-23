package com.lk.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.SendTo;

import java.util.Arrays;
import java.util.stream.Collectors;

@SpringBootApplication
public class KafkaDemo1Application {

  public static void main(String[] args) {
    SpringApplication.run(KafkaDemo1Application.class, args);
  }


  /**
   * Springboot继承kafka
   */
  @KafkaListener(topics = {"topic01"})
  @SendTo(value = "topic02")
  public String listenner(ConsumerRecord<?, ?> consumerRecord) {
    return consumerRecord.value() + "重新转发!";
  }

  @Bean
  public KStream<String, String> kStream(StreamsBuilder kStreamBuilder) {
    KStream<String, String> stream = kStreamBuilder.stream(
        "topic02",
        Consumed.with(Serdes.String(),
            Serdes.String()));

    stream.flatMapValues(new ValueMapper<String, Iterable<String>>() {
      @Override
      public Iterable<String> apply(String s) {
        return Arrays.stream(s.split(" ")).collect(Collectors.toList());
      }
    })
        .selectKey((k, v) -> v)
        .groupByKey(Serialized.with(Serdes.String(), Serdes.String()))
        .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("wordcount"))
        .toStream()
        .print(Printed.toSysOut());

    return stream;
  }

}
