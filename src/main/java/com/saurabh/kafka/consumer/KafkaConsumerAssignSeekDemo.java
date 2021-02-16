package com.saurabh.kafka.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaConsumerAssignSeekDemo {
	private static final String GROUP_ID = "my-third-app";

	static Logger logger = LoggerFactory.getLogger(KafkaConsumerAssignSeekDemo.class);

	final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
	static final String TOPIC_NAME = "first_topic_2";

	public static void main(String[] args) throws InterruptedException, ExecutionException {

		KafkaConsumerAssignSeekDemo kafkaConsumerDemo = new KafkaConsumerAssignSeekDemo();

		// create kafka consumer properties
		Properties consumerProperties = kafkaConsumerDemo.createKafkaConsumerProperties();

		// create kafka consumer
		KafkaConsumer<String, String> kafkaConsumer = kafkaConsumerDemo.createConsumer(consumerProperties);

		// assign and seek is used to access specific data 
		TopicPartition partitions = new TopicPartition(TOPIC_NAME, 0);
		kafkaConsumer.assign(Arrays.asList(partitions));
		
		// now seek
		kafkaConsumer.seek(partitions, 10);

		logger.info("Record received: \n");
		// poll for record
		while (true) {
			ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
			kafkaConsumerDemo.processRecords(records);
		}
	}

	private void processRecords(ConsumerRecords<String, String> records) {
		for (ConsumerRecord<String, String> record : records) {
			logger.info("\nTopic: " + record.topic() + "\n" + "Partition: " + record.partition() + "\n" + "Key: "
					+ record.key() + "\n" + "Value: " + record.value() + "\n" + "Offset: " + record.offset() + "\n"
					+ "Timestamp: " + record.timestamp());
		}

	}

	public Properties createKafkaConsumerProperties() {
		// create kafka consumer properties
		Properties consumerProperties = new Properties();
		consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				StringDeserializer.class.getName());
		consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				StringDeserializer.class.getName());
		consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		return consumerProperties;
	}

	public KafkaConsumer<String, String> createConsumer(Properties consumerProperties) {
		// create kafka consumer
		KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(consumerProperties);
		return kafkaConsumer;
	}

}
