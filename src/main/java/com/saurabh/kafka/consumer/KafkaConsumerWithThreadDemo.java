package com.saurabh.kafka.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaConsumerWithThreadDemo {
	private static final String GROUP_ID = "my-third-app";

	static Logger logger = LoggerFactory.getLogger(KafkaConsumerWithThreadDemo.class);

	final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
	static final String TOPIC_NAME = "first_topic_2";

	public static void main(String[] args) throws InterruptedException, ExecutionException {

		KafkaConsumerWithThreadDemo consumerWithThreadDemo = new KafkaConsumerWithThreadDemo();
		consumerWithThreadDemo.startConsumer();

	}

	public void startConsumer() {
		logger.info("Creating consumer thread");
		CountDownLatch latch = new CountDownLatch(1);
		Runnable consumerRunnable = new ConsumerRunnable(latch);
		Thread consumerThread = new Thread(consumerRunnable);
		consumerThread.start();

		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			logger.info("Got shutdown instruction");
			((ConsumerRunnable) consumerRunnable).shutdown();
			try {
				latch.await();
			} catch (InterruptedException e) {
				logger.error(e.getMessage(), e);
			}
			logger.info("Application exited");
		}));
		try {
			latch.await();
		} catch (InterruptedException e) {
			logger.error("Application is interrupted:", e);
		} finally {
			logger.info("Application is closing");
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
		consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
		return consumerProperties;
	}

	public KafkaConsumer<String, String> createConsumer(Properties consumerProperties) {
		// create kafka consumer
		KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(consumerProperties);
		return kafkaConsumer;
	}

	public class ConsumerRunnable implements Runnable {

		private CountDownLatch latch;
		private KafkaConsumer<String, String> kafkaConsumer;
		Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);

		public ConsumerRunnable(CountDownLatch latch) {
			this.latch = latch;
			KafkaConsumerWithThreadDemo kafkaConsumerDemo = new KafkaConsumerWithThreadDemo();
			// create kafka consumer properties
			Properties consumerProperties = kafkaConsumerDemo.createKafkaConsumerProperties();

			// create kafka consumer
			kafkaConsumer = kafkaConsumerDemo.createConsumer(consumerProperties);

			// subscribe to topic
			kafkaConsumer.subscribe(Arrays.asList(TOPIC_NAME));
		}

		public void run() {
			try {
				// poll for record
				while (true) {
					ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
					processRecords(records);
				}
			} catch (WakeupException exception) {
				logger.info("Received shutdown signal");
			} finally {
				kafkaConsumer.close();
				// done with consumer
				latch.countDown();
			}
		}

		public void shutdown() {
			kafkaConsumer.wakeup();
		}

		private void processRecords(ConsumerRecords<String, String> records) {
			for (ConsumerRecord<String, String> record : records) {
				logger.info("\nTopic: " + record.topic() + "\n" + "Partition: " + record.partition() + "\n" + "Key: "
						+ record.key() + "\n" + "Value: " + record.value() + "\n" + "Offset: " + record.offset() + "\n"
						+ "Timestamp: " + record.timestamp());
			}

		}

	}
}
