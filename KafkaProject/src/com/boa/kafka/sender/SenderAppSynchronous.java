package com.boa.kafka.sender;

import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class SenderAppSynchronous {
	
	public static void main(String[] args) {
		
		//String topicName = "first-topic";
		String topicName = "third-topic";
		
		Properties props = new Properties();
		props.setProperty("bootstrap.servers", "localhost:9092");
		props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
		
		ProducerRecord<String, String> prodRec = new ProducerRecord<String, String>(topicName,
				"msg-1","This is Test msg");
		
		//asynchronous
		//'message delivered' msg may come after 'message sent' message
		/*producer.send(prodRec, new Callback() {
			
			@Override
			public void onCompletion(RecordMetadata arg0, Exception arg1) {
				// TODO Auto-generated method stub
				System.out.println("message delivery complete..");
			}
		});*/
		
		//synchronous
		//'message delivery' will come before 'message sent' msg
		//means sender will wait for delivery confirm before proceeding
		Future<RecordMetadata> future = producer.send(prodRec);
		
		try {
			future.get();
			System.out.println("message delivered..");
		} catch (InterruptedException | ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println("message sent...");
		//in.close();
		producer.close();
		
	}

}
