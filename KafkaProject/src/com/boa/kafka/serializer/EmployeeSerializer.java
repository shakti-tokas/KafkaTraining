package com.boa.kafka.serializer;

import java.io.StringWriter;
import java.util.Map;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import org.apache.kafka.common.serialization.Serializer;

import com.boa.kafka.domain.Employee;

public class EmployeeSerializer implements Serializer<Employee> {

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
		
		try {
			
			JAXBContext context = JAXBContext.newInstance(Employee.class);
			Marshaller marshaller = context.createMarshaller();
			StringWriter writer = new StringWriter();
			
			marshaller.marshal(employee, writer);
			String xmlContent = writer.toString();
			System.out.println("serializing... "+xmlContent);
			
			array = xmlContent.getBytes();
		}
		catch(JAXBException e){
			e.printStackTrace();
		}
		return array;
	}

}
