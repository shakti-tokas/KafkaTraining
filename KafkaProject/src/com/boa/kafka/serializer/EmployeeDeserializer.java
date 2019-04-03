package com.boa.kafka.serializer;

import java.io.StringReader;
import java.util.Map;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.apache.kafka.common.serialization.Deserializer;

import com.boa.kafka.domain.Employee;

public class EmployeeDeserializer implements Deserializer<Employee> {

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
		String xmlContent = new String(array);
		try {
			JAXBContext context = JAXBContext.newInstance(Employee.class);
			Unmarshaller unmarshaller = context.createUnmarshaller();
			StringReader reader = new StringReader(xmlContent);
			
			System.out.println("deserializing ... " + xmlContent);
			employee = (Employee)unmarshaller.unmarshal(reader);
			
		} catch (JAXBException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return employee;
	}

}
