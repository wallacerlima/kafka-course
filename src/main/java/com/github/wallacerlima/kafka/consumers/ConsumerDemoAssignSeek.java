package com.github.wallacerlima.kafka.consumers;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoAssignSeek {
	
	public static void main(String[] args) { 
		Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);
		
		String bs = "localhost:9092";
		String topic = "first_topic";
		
		Properties props = new Properties();
		props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bs);
		props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		TopicPartition partition = new TopicPartition(topic, 0);
		
		long offsetToReadFrom = 15L;
		consumer.assign(Arrays.asList(partition));
		
		consumer.seek(partition, offsetToReadFrom);
		
		int numberOfMessagesToRead = 5;
		boolean keepOnReading = true;
		int numberOfMessagesReadSoFar = 0;
		
		while(keepOnReading) {
			ConsumerRecords<String, String> records = 
					consumer.poll(Duration.ofMillis(100));
			
			for(ConsumerRecord<String, String> record : records) {
				numberOfMessagesReadSoFar += 1;
				
				logger.info("Key: " + record.key());
				logger.info("Value: " + record.value());
				logger.info("Partition: " + record.partition());
				logger.info("Offset: " + record.offset());
				if(numberOfMessagesReadSoFar >= numberOfMessagesToRead) {
					keepOnReading = false;
					break;
				}
			}
		}
		logger.info("Saindo da aplicação!");
	}
}
