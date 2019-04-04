package com.boa.kafka.sender;

import java.util.Properties;
import java.util.Scanner;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class SecureSenderApp {
	
	public static void main(String[] args) {
		
		//String topicName = "first-topic";
		String topicName = "third-topic";
		
		Properties props = new Properties();
		props.setProperty("bootstrap.servers", "localhost:9092");
		props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		
		//adding for security
		props.setProperty("security.protocol", "SASL_PLAINTEXT");
		props.setProperty("sasl.mechanism","PLAIN");
		
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
		//Scanner in = new Scanner(System.in);
		
		//Asynchronous way of sending
		for(int i=0;i<=4;i++) {
			
			//String s = in.nextLine();
			//String msgKey = "msg-"+i;
			//ProducerRecord<String, String> prodRec = new ProducerRecord<String, String>(topicName,
				//	"test-message","This is a test message from shz");
			//ProducerRecord<String, String> prodRec = new ProducerRecord<String, String>(topicName,
				//	"test-message",s);
			
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
		//in.close();
		producer.close();
		
	}

}
