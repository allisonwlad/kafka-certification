package com.github.allisonwlad.kafka.tutorial1;

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

public class ConsumerDemoWithThreads {
	public static void main(String[] args) {
		new ConsumerDemoWithThreads().run();
	}
	private ConsumerDemoWithThreads(){
		
	}
	private void run(){
		Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThreads.class.getName());
		
		String topic = "first-topic";
		String groupId = "Grupo-kafka-Thread";
		String bootstrapServer = "localhost:9092";
		CountDownLatch latch = new CountDownLatch(1);
		
		Runnable myConsumerRunnable = new ConsumerRunnable(latch, topic, bootstrapServer, groupId);
		
		// start thread
		Thread thread = new Thread(myConsumerRunnable);
		thread.start();
		
		Runtime.getRuntime().addShutdownHook(new Thread(()-> {
			logger.info("caugth shutdown hook ");
			((ConsumerRunnable) myConsumerRunnable).shutdown();
			try {
				latch.await();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	
		} ));
		try {
			latch.await();
		} catch (InterruptedException e) {
			logger.error("Application error ", e);
		}finally {
			logger.info("Application is closed");
		}
	}
	public class ConsumerRunnable implements Runnable {

		private CountDownLatch latch;
		private KafkaConsumer<String, String> consumer;
		private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());
		
		public ConsumerRunnable(CountDownLatch latch, String topic, String bootstrapServer, String groupId){
			this.latch=latch;
			Properties properties = new Properties();
			
			properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
			properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
			properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			consumer = new KafkaConsumer<String, String>(properties);
			
			// subscribe the consumer
			consumer.subscribe(Collections.singleton(topic));
		}
		public void run() {
			//pull new data
			try{
				while (true) {
					ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
				
					for(ConsumerRecord<String, String> record : records){
						logger.info("Key: "+ record.key());
						logger.info("Value: "+ record.value());
						logger.info("Partition: "+ record.partition());
						logger.info("Offset: "+ record.offset());
					}
				}
			}catch(WakeupException e){
				logger.info("received shutdown signal");
			}finally {
				consumer.close();
				latch.countDown();
			}
		}
		public void shutdown(){
			consumer.wakeup();
		}
		
	}
}
