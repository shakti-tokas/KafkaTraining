package com.boa.kafka.sender;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class SenderWithCustomPartitionerApp {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String topicName = "third-topic";
		
		Properties props = new Properties();
		props.setProperty("bootstrap.servers", "localhost:9092");
		props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.setProperty("partitioner.class", "com.boa.kafka.partitioner.MessagePartitioner");
		
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
		
		for(int i=0;i<=5;i++) {
			
			ProducerRecord<String, String> prodRec = new ProducerRecord<String, String>(topicName,
					"msg-1","Test msg for key 1 partition & group.");
					
			producer.send(prodRec);
			
			prodRec = new ProducerRecord<String, String>(topicName,
					"msg-2","Test msg for key 2 partition & group.");
					
			producer.send(prodRec);
			
			prodRec = new ProducerRecord<String, String>(topicName,
					"msg-3","Test msg for key 3 partition & group.");
					
			producer.send(prodRec);
		}
		
		System.out.println("message sent...");
		producer.close();

	}

}
