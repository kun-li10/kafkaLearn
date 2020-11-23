package com.lk.kafka.service;

/**
 * @author lk
 * @version 1.0
 * @date 2020/11/22 21:56
 */
public interface OrderService {

  void saveOrder(String key, String message);
}
