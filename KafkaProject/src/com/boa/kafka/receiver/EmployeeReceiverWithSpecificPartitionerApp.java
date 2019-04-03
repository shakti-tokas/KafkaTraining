package com.boa.kafka.receiver;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import com.boa.kafka.domain.Employee;

public class EmployeeReceiverWithSpecificPartitionerApp {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String topicName = "fifth-topic";
		
		Properties props = new Properties();
		props.setProperty("bootstrap.servers", "localhost:9092");
		props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.setProperty("value.deserializer", "com.boa.kafka.serializer.EmployeeJsonDeserializer");
		
		props.setProperty("group.id", args[0]);
		
		KafkaConsumer<String, Employee> consumer = new KafkaConsumer<String, Employee>(props);
		
		List<TopicPartition> partitionList = new ArrayList<TopicPartition>();
		int partitionNum = Integer.parseInt(args[1]);
		System.out.println("I am employee recevier that will receive from partiton no. " + partitionNum);
		
		TopicPartition partition = new TopicPartition(topicName, partitionNum);
		partitionList.add(partition);
		consumer.assign(partitionList);
		
		while(true){
			ConsumerRecords<String, Employee> records = consumer.poll(Duration.ofSeconds(10));
			for(ConsumerRecord<String, Employee> record:records){
				Employee employee = record.value();
				System.out.println("Received Employee Data: "+ employee.getId()+"\t"+ employee.getName()+"\t"+ employee.getDesignation());
				System.out.println("-- from partition "+record.partition()+" at offset "+record.offset());
			}
		}

	}

}
