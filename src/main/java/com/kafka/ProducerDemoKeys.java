package com.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hello world!
 *
 */
public class ProducerDemoKeys {
	public static void main(String[] args) {

		final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);
		final String topic = "first_topic";

		final String bootstrapServers = "127.0.0.1:9092";
		// Producer Properties
		Properties properties = new Properties();

		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// Producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

		for (int i = 0; i < 10; i++) {
			
			String value = "hello java" + Integer.toString(i);
			String key = "id_" + Integer.toString(i);
			
			// create a record
			ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value );

			// send data asynchronous
			producer.send(record, new Callback() {

				public void onCompletion(RecordMetadata recordmetadata, Exception e) {
					// TODO Auto-generated method stub
					// executes every time a record is successfully sent or an exception is thrown
					if (e == null) {
						logger.info("Received new Metadata. \n" + "Topic:" + recordmetadata.topic() + "\n"
								+ "Partition:" + recordmetadata.partition() + "\n" + "Offset:" + recordmetadata.offset()
								+ "\n" + "Timestamp:" + recordmetadata.timestamp() + "\n");
					} else {
						logger.error("Error while producing", e);
					}
				}
			});
		}

		producer.flush();

		producer.close();

	}
}
