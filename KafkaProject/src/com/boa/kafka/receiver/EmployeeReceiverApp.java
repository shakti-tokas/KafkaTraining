package com.boa.kafka.receiver;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.boa.kafka.domain.Employee;

public class EmployeeReceiverApp {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String topicName = "fifth-topic";
		
		Properties props = new Properties();
		props.setProperty("bootstrap.servers", "localhost:9092");
		props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		//props.setProperty("value.deserializer", "com.boa.kafka.serializer.EmployeeDeserializer");	//For XML content serialization
		props.setProperty("value.deserializer", "com.boa.kafka.serializer.EmployeeJsonDeserializer");	//For JSON content serialization
		props.setProperty("group.id", "demo-group");
		
		KafkaConsumer<String, Employee> consumer = new KafkaConsumer<String, Employee>(props);
		List<String> topicList = new ArrayList<String>();
		topicList.add(topicName);
		consumer.subscribe(topicList);
		
		while(true){
			ConsumerRecords<String, Employee> records = consumer.poll(Duration.ofSeconds(10));
			for(ConsumerRecord<String, Employee> record:records){
				Employee employee = record.value();
				//System.out.println(employee.toString());
				System.out.println("Received Employee Data: "+ employee.getId()+"\t"+ employee.getName()+"\t"+ employee.getDesignation());
			}
		}
		
	}

}
