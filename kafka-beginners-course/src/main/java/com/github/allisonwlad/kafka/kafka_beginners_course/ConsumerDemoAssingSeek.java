package com.github.allisonwlad.kafka.kafka_beginners_course;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoAssingSeek {
	public static void main(String[] args) {
		Logger logger = LoggerFactory.getLogger(ConsumerDemoAssingSeek.class.getName());
		String topic = "first-topic";
		// consumer configs
		Properties properties = new Properties();
		
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		// create the consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
		
		//assing and seek
		TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
		long offsetToReadFrom = 15L;
		consumer.assign(Arrays.asList(partitionToReadFrom));
		
		//seek 
		consumer.seek(partitionToReadFrom, offsetToReadFrom);
		
		int messagesToRead = 5;
		int messagesSoFar = 0;
		boolean keepOnReading = true;
		//pull new data
		while (keepOnReading) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			
			for(ConsumerRecord<String, String> record : records){
				messagesSoFar += 1;
				logger.info("Key: "+ record.key());
				logger.info("Value: "+ record.value());
				logger.info("Partition: "+ record.partition());
				logger.info("Offset: "+ record.offset());
				if(messagesSoFar > messagesToRead){
					keepOnReading = false;
					break;
				}
			}
		}
		logger.info("Exiting");
	}
}
