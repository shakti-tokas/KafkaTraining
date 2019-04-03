package com.boa.kafka.receiver;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

public class ReceiverWithSpecificPartitionerApp {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String topicName = "fourth-topic";
		
		Properties props = new Properties();
		props.setProperty("bootstrap.servers", "localhost:9092");
		props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		//props.setProperty("group.id", "demo-group");
		props.setProperty("group.id", args[0]);
		
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		
		List<TopicPartition> partitionList = new ArrayList<TopicPartition>();
		int partitionNum = Integer.parseInt(args[1]);
		System.out.println("I am recevier that will receive from partiton no. "+partitionNum);
		
		TopicPartition partition = new TopicPartition(topicName, partitionNum);
		partitionList.add(partition);
		consumer.assign(partitionList);
		
		//to start read from a particular offset, use 0 to start read from beginning till end
		consumer.seek(partition, 4);
		
		while(true) {
			ConsumerRecords<String, String> conRecds = consumer.poll(Duration.ofSeconds(10));
			for(ConsumerRecord<String, String> recs : conRecds) {
				//System.out.println("received message: "+recs.value());
				System.out.println("received message: "+recs.value()+
						" from partition "+recs.partition()+" at offset "+recs.offset());
			}
		}

	}

}
