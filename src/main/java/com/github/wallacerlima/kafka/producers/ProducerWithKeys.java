package com.github.wallacerlima.kafka.producers;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerWithKeys {
	
	public static void main(String[] args) {
		
		Logger logger = LoggerFactory.getLogger(ProducerWithCallback.class);
		
		Properties props = new Properties();
		props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
		
		for (int i = 0; i < 10; i++) {
			
			String topic = "first_topic";
			String value = "Hello Java! " + Integer.toString(i);
			String key = "id_" + Integer.toString(i);
					
			ProducerRecord<String, String> record = 
					new ProducerRecord<String, String>(topic, key, value);
			
			logger.info("Key: " + key);
			
			producer.send(record, new Callback() {
				@Override
				public void onCompletion(RecordMetadata metadata, Exception exception) {
					if (exception == null) {
						logger.info("Received new metadata.");
						logger.info("Topic: " + metadata.topic());
						logger.info("Partition: " + metadata.partition());
						logger.info("Offset: " + metadata.offset());
						logger.info("Timestamp: " + metadata.timestamp());
					} else {
						logger.error("Erro:", exception);
					}
				}
			});
			
			producer.flush();
		}
		producer.close();
	}
}