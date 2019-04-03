package com.boa.kafka.sender;

import java.util.Properties;
import java.util.Scanner;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.boa.kafka.domain.Employee;

public class EmployeeSenderApp {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		String topicName = "fifth-topic";
		
		Properties props = new Properties();
		props.setProperty("bootstrap.servers", "localhost:9092");
		props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		//props.setProperty("value.serializer", "com.boa.kafka.serializer.EmployeeSerializer"); //For XML content serialization
		props.setProperty("value.serializer", "com.boa.kafka.serializer.EmployeeJsonSerializer"); //For JSON content serialization
		
		KafkaProducer<String, Employee> producer = new KafkaProducer<String, Employee>(props);
		Scanner in = new Scanner(System.in);
		
		//get employee details
		System.out.println("enter id");
		int id = Integer.parseInt(in.nextLine());
		String name = in.nextLine();
		String designation = in.nextLine();
		Employee emp = new Employee(id, name, designation);
		
		ProducerRecord<String, Employee> empRec = new ProducerRecord<String, Employee>(topicName, "emp-id-"+id, emp);
		producer.send(empRec, new Callback() {
			
			@Override
			public void onCompletion(RecordMetadata rmd, Exception e) {
				// TODO Auto-generated method stub
				if(e==null){
					System.out.println("message delivered to "+ rmd.partition()+" at offset "+ rmd.offset());
				}
			}
		});
		
		System.out.println("Employee message sent...");
		producer.close();

	}

}
