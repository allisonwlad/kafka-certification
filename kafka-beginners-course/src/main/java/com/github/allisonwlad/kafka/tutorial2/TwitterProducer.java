package com.github.allisonwlad.kafka.tutorial2;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterProducer {
	Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());
	String consumerKey = "0aMKi0Ip37arQZISnVcq3dLMS";
	String consumerSecret = "82J4ZkKzS9PbWrh0MVoC9hO5T9Et1QVh4wIk5XJnBVKeHU62aT";
	String token = "1596057121-KoRTTR8XOFB2S4fyvLlZaTsqgMMZWCq7q9LeIdh";
	String secret = "hHJFuT7qg58cpPCl1qPeIKAbgA9mhB44s80n1NbjSzqAp";
	
	public TwitterProducer(){}

	public static void main(String[] args) {
		new TwitterProducer().run();
	}
	
	public void run(){
		//twitter client
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
		Client client = createTwitterClient(msgQueue);
		// connect client
		client.connect();
		
		//kafka producer
		KafkaProducer producer = createKafkaProducer();
		
		//add shutdonwhooker
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			logger.info("stopping application");
			logger.info("stopping client");
			client.stop();
			logger.info("close producer");
			producer.close();
			logger.info("done");
		}));
		
		//loop send twitts to kafka 	
		//diferentes threads
		while (!client.isDone()) {
			String msg = null;
			try{
			  msg = msgQueue.poll(5, TimeUnit.SECONDS);
			}catch(InterruptedException e){
				e.printStackTrace();
				client.stop();
			}
			  
			if(msg != null){
				logger.info(msg);
				producer.send(new ProducerRecord<String, String>("twetts", null, msg), new Callback() {
					@Override
					public void onCompletion(RecordMetadata recordMetadata, Exception e) {
						// TODO Auto-generated method stub
						if(e != null){
							logger.error("erro ao enviar mensagem", e);
						}
					}
				});
			}
			
		}
		logger.info("fim da aplicação");
	

	}
	public Client createTwitterClient(BlockingQueue<String> msgQueue){
			
		/** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		// Optional: set up some followings and track terms
		List<String> terms = Lists.newArrayList("bitcoin");
		hosebirdEndpoint.trackTerms(terms);

		// These secrets should be read from a config file
		Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);
		
		ClientBuilder builder = new ClientBuilder()
				  .name("Hosebird-Client-01")                              // optional: mainly for the logs
				  .hosts(hosebirdHosts)
				  .authentication(hosebirdAuth)
				  .endpoint(hosebirdEndpoint)
				  .processor(new StringDelimitedProcessor(msgQueue));

		Client hosebirdClient = builder.build();
		return hosebirdClient	;
	}
	public KafkaProducer<String, String> createKafkaProducer(){
		Properties properties = new Properties();
		
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		//safe Producer
		properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
		properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
		properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
		properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
		//create the producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		return producer;
				
	}
}
