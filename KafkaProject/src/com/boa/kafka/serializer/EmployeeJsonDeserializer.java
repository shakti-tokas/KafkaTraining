package com.boa.kafka.serializer;

import java.io.IOException;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import com.boa.kafka.domain.Employee;
import com.fasterxml.jackson.databind.ObjectMapper;

public class EmployeeJsonDeserializer implements Deserializer<Employee> {

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void configure(Map<String, ?> arg0, boolean arg1) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Employee deserialize(String topicName, byte[] array) {
		// TODO Auto-generated method stub
		Employee employee = null;
		String jsonContent = new String(array);
		
		ObjectMapper mapper = new ObjectMapper();
		System.out.println("Deserializing.. " + jsonContent);
		
		try {
			
			employee = mapper.readValue(jsonContent, Employee.class);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return employee;
	}

}
