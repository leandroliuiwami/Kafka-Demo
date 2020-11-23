package com.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Hello world!
 *
 */
public class ProducerDemo {
	public static void main(String[] args) {
		final String bootstrapServers = "127.0.0.1:9092";
		// Producer Properties
		Properties properties = new Properties();

		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// Producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		
		// create a record
		ProducerRecord<String , String> record = new ProducerRecord<String, String>("first_topic", "hello java");
		
		
		// send data asynchronous
		producer.send(record);
		
		producer.flush();
		
		producer.close();
		
	}
}
