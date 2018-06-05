/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.producer;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

/**
 * 发送给 Kafka 的 key/value ， 包含 topic 名字，可选的 partition 序号，可选的 key value
 * <p>
 * A key/value pair to be sent to Kafka. This consists of a topic name to which the record is being sent, an optional
 * partition number, and an optional key and value.
 * <p>
 * 当一个合法的 partition 序号被指定时，该 partition 序号就会在发送时被使用。如果没有 partition 序号被指定，但是 key 存在时，partition
 * 序号会通过 key 的 hash 值来选择。如果 partition 序号和 key 都没有指定时，partition 序号会通过 round-robin fashion（轮询调度算法） 来生成。
 * If a valid partition number is specified that partition will be used when sending the record. If no partition is
 * specified but a key is present a partition will be chosen using a hash of the key. If neither key nor partition is
 * present a partition will be assigned in a round-robin fashion.
 * <p>
 * record 也会有一个关联的时间戳。如果用户没有提供时间戳，生产者会以当前的时间填充。 时间戳最终会被 Kafka 以 topic TimestampType的配置来使用
 * The record also has an associated timestamp. If the user did not provide a timestamp, the producer will stamp the
 * record with its current time. The timestamp eventually used by Kafka depends on the timestamp type configured for
 * the topic.
 * <li>
 * 如果 topic 配置 {@link org.apache.kafka.common.record.TimestampType#CREATE_TIME CreateTime} 那么生产者的时间戳会被 broker 使用
 * If the topic is configured to use {@link org.apache.kafka.common.record.TimestampType#CREATE_TIME CreateTime},
 * the timestamp in the producer record will be used by the broker.
 * </li>
 * <li>
 * 如果 topic 配置 {@link org.apache.kafka.common.record.TimestampType#LOG_APPEND_TIME LogAppendTime} 那么当消息附加到 log 时，生产者的时间戳
 * 会被 broker 本地的时间所覆盖。
 * If the topic is configured to use {@link org.apache.kafka.common.record.TimestampType#LOG_APPEND_TIME LogAppendTime},
 * the timestamp in the producer record will be overwritten by the broker with the broker local time when it appends the
 * message to its log.
 * </li>
 * <p>
 * 无论是上面哪一种方法，最终实际本使用的时间戳会通过 {@link RecordMetadata} 返回给用户
 * In either of the cases above, the timestamp that has actually been used will be returned to user in
 * {@link RecordMetadata}
 */
public class ProducerRecord<K, V> {

    private final String topic;         // topic 名字
    private final Integer partition;    // 分区号
    private final Headers headers;
    private final K key;
    private final V value;
    private final Long timestamp;

    /**
     * Creates a record with a specified timestamp to be sent to a specified topic and partition
     * 
     * @param topic The topic the record will be appended to
     * @param partition The partition to which the record should be sent
     * @param timestamp The timestamp of the record, in milliseconds since epoch. If null, the producer will assign
     *                  the timestamp using System.currentTimeMillis().
     * @param key The key that will be included in the record
     * @param value The record contents
     * @param headers the headers that will be included in the record
     */
    public ProducerRecord(String topic, Integer partition, Long timestamp, K key, V value, Iterable<Header> headers) {
        if (topic == null)
            throw new IllegalArgumentException("Topic cannot be null.");
        if (timestamp != null && timestamp < 0)
            throw new IllegalArgumentException(
                    String.format("Invalid timestamp: %d. Timestamp should always be non-negative or null.", timestamp));
        if (partition != null && partition < 0)
            throw new IllegalArgumentException(
                    String.format("Invalid partition: %d. Partition number should always be non-negative or null.", partition));
        this.topic = topic;
        this.partition = partition;
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
        this.headers = new RecordHeaders(headers);
    }

    /**
     * Creates a record with a specified timestamp to be sent to a specified topic and partition
     *
     * @param topic The topic the record will be appended to
     * @param partition The partition to which the record should be sent
     * @param timestamp The timestamp of the record, in milliseconds since epoch. If null, the producer will assign the
     *                  timestamp using System.currentTimeMillis().
     * @param key The key that will be included in the record
     * @param value The record contents
     */
    public ProducerRecord(String topic, Integer partition, Long timestamp, K key, V value) {
        this(topic, partition, timestamp, key, value, null);
    }

    /**
     * Creates a record to be sent to a specified topic and partition
     *
     * @param topic The topic the record will be appended to
     * @param partition The partition to which the record should be sent
     * @param key The key that will be included in the record
     * @param value The record contents
     * @param headers The headers that will be included in the record
     */
    public ProducerRecord(String topic, Integer partition, K key, V value, Iterable<Header> headers) {
        this(topic, partition, null, key, value, headers);
    }
    
    /**
     * Creates a record to be sent to a specified topic and partition
     *
     * @param topic The topic the record will be appended to
     * @param partition The partition to which the record should be sent
     * @param key The key that will be included in the record
     * @param value The record contents
     */
    public ProducerRecord(String topic, Integer partition, K key, V value) {
        this(topic, partition, null, key, value, null);
    }
    
    /**
     * Create a record to be sent to Kafka
     * 
     * @param topic The topic the record will be appended to
     * @param key The key that will be included in the record
     * @param value The record contents
     */
    public ProducerRecord(String topic, K key, V value) {
        this(topic, null, null, key, value, null);
    }
    
    /**
     * Create a record with no key
     * 
     * @param topic The topic this record should be sent to
     * @param value The record contents
     */
    public ProducerRecord(String topic, V value) {
        this(topic, null, null, null, value, null);
    }

    /**
     * @return The topic this record is being sent to
     */
    public String topic() {
        return topic;
    }

    /**
     * @return The headers
     */
    public Headers headers() {
        return headers;
    }

    /**
     * @return The key (or null if no key is specified)
     */
    public K key() {
        return key;
    }

    /**
     * @return The value
     */
    public V value() {
        return value;
    }

    /**
     * @return The timestamp, which is in milliseconds since epoch.
     */
    public Long timestamp() {
        return timestamp;
    }

    /**
     * @return The partition to which the record will be sent (or null if no partition was specified)
     */
    public Integer partition() {
        return partition;
    }

    @Override
    public String toString() {
        String headers = this.headers == null ? "null" : this.headers.toString();
        String key = this.key == null ? "null" : this.key.toString();
        String value = this.value == null ? "null" : this.value.toString();
        String timestamp = this.timestamp == null ? "null" : this.timestamp.toString();
        return "ProducerRecord(topic=" + topic + ", partition=" + partition + ", headers=" + headers + ", key=" + key + ", value=" + value +
            ", timestamp=" + timestamp + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        else if (!(o instanceof ProducerRecord))
            return false;

        ProducerRecord<?, ?> that = (ProducerRecord<?, ?>) o;

        if (key != null ? !key.equals(that.key) : that.key != null) 
            return false;
        else if (partition != null ? !partition.equals(that.partition) : that.partition != null) 
            return false;
        else if (topic != null ? !topic.equals(that.topic) : that.topic != null) 
            return false;
        else if (headers != null ? !headers.equals(that.headers) : that.headers != null)
            return false;
        else if (value != null ? !value.equals(that.value) : that.value != null) 
            return false;
        else if (timestamp != null ? !timestamp.equals(that.timestamp) : that.timestamp != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = topic != null ? topic.hashCode() : 0;
        result = 31 * result + (partition != null ? partition.hashCode() : 0);
        result = 31 * result + (headers != null ? headers.hashCode() : 0);
        result = 31 * result + (key != null ? key.hashCode() : 0);
        result = 31 * result + (value != null ? value.hashCode() : 0);
        result = 31 * result + (timestamp != null ? timestamp.hashCode() : 0);
        return result;
    }
}
