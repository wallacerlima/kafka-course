package com.github.wallacerlima.kafka.consumers;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoGroups {
	
	public static void main(String[] args) { 
		Logger logger = LoggerFactory.getLogger(ConsumerDemoGroups.class);
		
		String bs = "localhost:9092";
		String groupId = "java-app";
		String topic = "first_topic";
		
		Properties props = new Properties();
		props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bs);
		props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		
		consumer.subscribe(Collections.singleton(topic));
		
		while(true) {
			ConsumerRecords<String, String> records = 
					consumer.poll(Duration.ofMillis(100));
			
			for(ConsumerRecord<String, String> record : records) {
				logger.info("Key: " + record.key());
				logger.info("Value: " + record.value());
				logger.info("Partition: " + record.partition());
				logger.info("Offset: " + record.offset());
			}
		}
	}
}
