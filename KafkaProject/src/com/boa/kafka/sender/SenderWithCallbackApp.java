package com.boa.kafka.sender;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class SenderWithCallbackApp {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String topicName = "fourth-topic";
		
		Properties props = new Properties();
		props.setProperty("bootstrap.servers", "localhost:9092");
		props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.setProperty("partitioner.class", "com.kafka.partitioner.MessagePartitioner");
		
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
		MyCallback callback = new MyCallback();
		
		for(int i=0;i<=5;i++) {
			
			ProducerRecord<String, String> prodRec = new ProducerRecord<String, String>(topicName,
					"msg-1","Test msg for key 1 partition & group.");
					
			producer.send(prodRec, callback);
			
			prodRec = new ProducerRecord<String, String>(topicName,
					"msg-2","Test msg for key 2 partition & group.");
					
			producer.send(prodRec, callback);
			
			prodRec = new ProducerRecord<String, String>(topicName,
					"msg-3","Test msg for key 3 partition & group.");
					
			producer.send(prodRec, callback);
		}
		
		System.out.println("message sent...");
		producer.close();

	}

}

class MyCallback implements Callback {

	@Override
	public void onCompletion(RecordMetadata rmd, Exception e) {
		// TODO Auto-generated method stub
		if(e == null) {
			System.out.println("message published to "+rmd.partition()+" at offset "+rmd.offset());
		}
	}
	
	
}
