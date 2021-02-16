package com.saurabh.kafka.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaProducerDemo {
	final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
	final String TOPIC_NAME = "first_topic";

	public static void main(String[] args) {

		KafkaProducerDemo kafkaProducerDemo = new KafkaProducerDemo();

		// create kafka producer properties
		Properties producerProperties = kafkaProducerDemo.createKafkaProducerProperties();

		// create kafka producer
		KafkaProducer<String, String> kafkaProducer = kafkaProducerDemo.createProducer(producerProperties);

		// create kafka record
		ProducerRecord<String, String> record = kafkaProducerDemo.createRecord("My First Message From Java!!!");

		// send kafka record
		kafkaProducer.send(record);
		
		// flush record -- send is async so until flush or close is called message won't be produced
		kafkaProducer.flush();
		
		// flush and close record
		kafkaProducer.close();
	}
	
	public Properties createKafkaProducerProperties() {
		
		// create kafka producer properties 
		Properties producerProperties = new Properties();
		producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS );
		producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		return producerProperties;		
	}
	
	public KafkaProducer<String, String> createProducer(Properties producerProperties){
		// create kafka producer
		KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(producerProperties);
		return kafkaProducer;
	}
	
	public ProducerRecord<String, String> createRecord(String msg){
		// create kafka producer
		ProducerRecord<String, String> record = new ProducerRecord<String, String>(TOPIC_NAME, msg);
		return record;
	}
}
