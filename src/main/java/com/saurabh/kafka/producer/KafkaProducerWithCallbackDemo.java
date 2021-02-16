package com.saurabh.kafka.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaProducerWithCallbackDemo {
	static Logger logger = LoggerFactory.getLogger(KafkaProducerWithCallbackDemo.class);

	final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
	final String TOPIC_NAME = "first_topic_2";

	public static void main(String[] args) {

		KafkaProducerWithCallbackDemo kafkaProducerDemo = new KafkaProducerWithCallbackDemo();

		// create kafka producer properties
		Properties producerProperties = kafkaProducerDemo.createKafkaProducerProperties();

		// create kafka producer
		KafkaProducer<String, String> kafkaProducer = kafkaProducerDemo.createProducer(producerProperties);

		for (int i = 0; i < 10; i++) {
			// create kafka record
			ProducerRecord<String, String> record = kafkaProducerDemo.createRecord("My " + i + " Message From Java!!!");

			// send kafka record
			kafkaProducer.send(record, new Callback() {

				public void onCompletion(RecordMetadata metadata, Exception exception) {
					if (exception == null) {
						logger.info("Received new metadata \n" + "Topic: " + metadata.topic() + "\n" + "Partition: "
								+ metadata.partition() + "\n" + "Offset: " + metadata.offset() + "\n" + "Timestamp: "
								+ metadata.timestamp());
					} else {
						logger.error("Error on message send.", exception);
					}
				}
			});

		}
		// flush record -- send is async so until flush or close is called message won't
		// be produced
		kafkaProducer.flush();

		// flush and close record
		kafkaProducer.close();
	}

	public Properties createKafkaProducerProperties() {

		// create kafka producer properties
		Properties producerProperties = new Properties();
		producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		return producerProperties;
	}

	public KafkaProducer<String, String> createProducer(Properties producerProperties) {
		// create kafka producer
		KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(producerProperties);
		return kafkaProducer;
	}

	public ProducerRecord<String, String> createRecord(String msg) {
		// create kafka producer
		ProducerRecord<String, String> record = new ProducerRecord<String, String>(TOPIC_NAME, msg);
		return record;
	}
}
