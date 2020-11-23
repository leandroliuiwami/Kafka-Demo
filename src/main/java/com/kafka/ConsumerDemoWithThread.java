package com.kafka;

import java.time.Duration;
import java.util.Collection;
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
	
	private ConsumerDemoWithThread() {
		
	}
		
	private void run() {
			Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class);
			final String bootstrapServers = "127.0.0.1:9092";
			final String groupId = "my-sixth";
			final String topic = "first_topic";
			CountDownLatch latch = new CountDownLatch(1);
			
			logger.info("Creating the consumer!");
			Runnable myConsumerRunnable = new ConsumerRunnable(latch, bootstrapServers, topic, groupId);
			
			Thread myThread = new Thread(myConsumerRunnable);
			myThread.start();
			
			Runtime.getRuntime().addShutdownHook(new Thread(() -> {
				logger.info("Caught the shutdown hook");
				((ConsumerRunnable) myConsumerRunnable).shutdown();
			}
			
			));
			
			try {
				latch.await();
			}catch(InterruptedException e){
				logger.error("Application interrupted", e);
			}finally {
				logger.info("Application is closing");
			}
		}



	public class ConsumerRunnable implements Runnable {

		private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);
		private CountDownLatch latch;
		private KafkaConsumer<String, String> consumer;

		public ConsumerRunnable(CountDownLatch latch, String bootstrapServers, String topic, String groupId) {
			this.latch = latch;
			Properties properties = new Properties();

			// consumer group configs
			properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
			properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
			properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

			// create consumer
			logger.info("Creating a consumer fase 2");
			consumer = new KafkaConsumer<String, String>(properties);

			// subscribe consumer to the topic
			logger.info("Application is subscribe: " + topic);
			consumer.subscribe(Collections.singleton(topic));
		}

		public void run() {
			try {
				while (true) {

					ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

					for (ConsumerRecord<String, String> record : records) {
						logger.info("KEY: " + record.key() + ", Value: " + record.value());
						logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
					}
				}
			} catch (WakeupException e) {
				logger.info("Received shutdown signal");
			} finally {
				consumer.close();
				latch.countDown();
			}
		}

		public void shutdown() {
			// interrupt consumer poll
			consumer.wakeup();
		}
	}
}
