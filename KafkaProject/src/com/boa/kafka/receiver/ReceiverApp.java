package com.boa.kafka.receiver;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ReceiverApp {
	
	public static void main(String[] args) {
		
		//String topicName = "first-topic";
		String topicName = "third-topic";
		
		Properties props = new Properties();
		props.setProperty("bootstrap.servers", "localhost:9092");
		props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		//props.setProperty("group.id", "demo-group");
		props.setProperty("group.id", args[0]);
		
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		List<String> topicList = new ArrayList<String>();
		topicList.add(topicName);
		consumer.subscribe(topicList);
		
		while(true) {
			ConsumerRecords<String, String> conRecds = consumer.poll(Duration.ofSeconds(10));
			for(ConsumerRecord<String, String> recs : conRecds) {
				//System.out.println("received message: "+recs.value());
				System.out.println("received message: "+recs.value()+" from partition "+recs.partition());
			}
		}
	}

}
