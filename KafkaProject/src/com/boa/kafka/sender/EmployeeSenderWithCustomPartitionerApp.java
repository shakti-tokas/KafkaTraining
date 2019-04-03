package com.boa.kafka.sender;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.boa.kafka.domain.Employee;

public class EmployeeSenderWithCustomPartitionerApp {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String topicName = "fifth-topic";
		
		Properties props = new Properties();
		props.setProperty("bootstrap.servers", "localhost:9092");
		props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.setProperty("value.serializer", "com.boa.kafka.serializer.EmployeeJsonSerializer");
		props.setProperty("partitioner.class", "com.boa.kafka.partitioner.EmployeePartitioner");
		
		KafkaProducer<String, Employee> producer = new KafkaProducer<String, Employee>(props);
		
		Callback callback = new Callback() {
			
			@Override
			public void onCompletion(RecordMetadata rmd, Exception e) {
				// TODO Auto-generated method stub
				if(e==null){
					System.out.println("message delivered to "+rmd.partition()+" at offset "+rmd.offset());
				}
			}
		};
		
		for(int i=1001;i<=1005;i++){
			Employee e=new Employee(i, "Name "+i, "Developer");
			ProducerRecord<String, Employee> record=new ProducerRecord<String, Employee>(topicName, e);
			producer.send(record,callback);
		}
		for(int i=1006;i<=1008;i++){
			Employee e=new Employee(i, "Name "+i, "Accountant");
			ProducerRecord<String, Employee> record=new ProducerRecord<String, Employee>(topicName, e);
			producer.send(record,callback);
		}
		for(int i=1009;i<=1012;i++){
			Employee e=new Employee(i, "Name "+i, "Architect");
			ProducerRecord<String, Employee> record=new ProducerRecord<String, Employee>(topicName, e);
			producer.send(record,callback);
		}
		for(int i=1013;i<=1015;i++){
			Employee e=new Employee(i, "Name "+i, "Team Lead");
			ProducerRecord<String, Employee> record=new ProducerRecord<String, Employee>(topicName, e);
			producer.send(record,callback);
		}
		for(int i=1016;i<=1020;i++){
			Employee e=new Employee(i, "Name "+i, "Sys Admin");
			ProducerRecord<String, Employee> record=new ProducerRecord<String, Employee>(topicName, e);
			producer.send(record,callback);
		}
		
		producer.close();

	}

}
