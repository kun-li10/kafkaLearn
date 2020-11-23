package com.lk.kafka.serialize;

import java.io.Serializable;

/**
 * 自定义序列化
 *
 * @author lk
 * @version 1.0
 * @date 2020/11/22 20:01
 */
public class User implements Serializable {

  private static final long serialVersionUID = 8905174598339569635L;

  private String name;
  private String address;
  private int age;

  public User(String name, String address, int age) {
    this.name = name;
    this.address = address;
    this.age = age;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getAddress() {
    return address;
  }

  public void setAddress(String address) {
    this.address = address;
  }

  public int getAge() {
    return age;
  }

  public void setAge(int age) {
    this.age = age;
  }

  @Override
  public String toString() {
    return "User{" +
        "name='" + name + '\'' +
        ", address='" + address + '\'' +
        ", age=" + age +
        '}';
  }
}
