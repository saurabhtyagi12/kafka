package com.saurabh.kafka.producer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaProducerWithKeysDemo {
	static Logger logger = LoggerFactory.getLogger(KafkaProducerWithKeysDemo.class);

	final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
	final String TOPIC_NAME = "first_topic_2";

	public static void main(String[] args) throws InterruptedException, ExecutionException {

		KafkaProducerWithKeysDemo kafkaProducerDemo = new KafkaProducerWithKeysDemo();

		// create kafka producer properties
		Properties producerProperties = kafkaProducerDemo.createKafkaProducerProperties();

		// create kafka producer
		KafkaProducer<String, String> kafkaProducer = kafkaProducerDemo.createProducer(producerProperties);

		for (int i = 0; i < 10; i++) {
			// create kafka record
			String key = "key_"+i;
			logger.info("Key: "+key);
			ProducerRecord<String, String> record = kafkaProducerDemo.createRecord(key, "My " + i + " Message From Java!!!");

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
			}).get(); // this is to block the send // don't do this in production as it will have performance impact

		}
		/*
		key_0 goes to Partition: 2
		key_1 goes to Partition: 1
		key_2 goes to Partition: 0
		key_3 goes to Partition: 1
		key_4 goes to Partition: 0
		key_5 goes to Partition: 0
		key_6 goes to Partition: 2
		key_7 goes to Partition: 2
		key_8 goes to Partition: 2
		key_9 goes to Partition: 2
		*/
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

	public ProducerRecord<String, String> createRecord(String key, String msg) {
		// create kafka producer
		ProducerRecord<String, String> record = new ProducerRecord<String, String>(TOPIC_NAME, key, msg);
		return record;
	}
}
