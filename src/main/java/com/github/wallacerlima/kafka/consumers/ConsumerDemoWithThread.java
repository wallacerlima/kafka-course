package com.github.wallacerlima.kafka.consumers;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoWithThread {
	
	public static void main(String[] args) { 
		new ConsumerDemoWithThread().run();
	}
	
	private ConsumerDemoWithThread () {}
	
	private void run() {
		Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class);
		
		String bs = "localhost:9092";
		String groupId = "java-app-thread";
		String topic = "first_topic";
		
		CountDownLatch latch = new CountDownLatch(1);
		
		logger.info("Creating the consumer thread!");
		Runnable consumerRunnable = new ConsumerRunnable(latch, bs, groupId, topic);
		
		Thread thread = new Thread(consumerRunnable);
		thread.start();
		
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			logger.info("Shutdown hook!");
			
			((ConsumerRunnable) consumerRunnable).shutdown();
			try {
				latch.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			logger.info("Fim");
		}));
		
		try {
			latch.await();
		} catch (InterruptedException e) {
			logger.error("Erro: ", e);
		} finally {
			logger.info("App is closing");
		}
	}
	
	public class ConsumerRunnable implements Runnable {
	
		private CountDownLatch latch;
		private KafkaConsumer<String, String> consumer;
		private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());
		
		public ConsumerRunnable(CountDownLatch latch, 
							  String bs, 
							  String groupId,
							  String topic) {
			
			this.latch = latch;
			
			Properties props = new Properties();
			props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bs);
			props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
			props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			
			this.consumer = new KafkaConsumer<String, String>(props);
			
			consumer.subscribe(Collections.singleton(topic));
		}
			
		@Override
		public void run() {
			try {
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
			} catch (WakeupException e) {
				logger.info("Received shutdown signal!");
			} finally {
				consumer.close();
				latch.countDown();
			}
				
		}
			
		public void shutdown() {
			consumer.wakeup();
		}
			
	}
}