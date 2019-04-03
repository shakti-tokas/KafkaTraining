package com.boa.kafka.serializer;

import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;

import com.boa.kafka.domain.Employee;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class EmployeeJsonSerializer implements Serializer<Employee> {

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void configure(Map<String, ?> arg0, boolean arg1) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public byte[] serialize(String topicName, Employee employee) {
		// TODO Auto-generated method stub
		byte[] array = null;
		ObjectMapper mapper = new ObjectMapper();
		
		try {
			
			String jsonContent = mapper.writeValueAsString(employee);
			System.out.println("Serializing... " + jsonContent);
			array = jsonContent.getBytes();
			
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return array;
	}

}
