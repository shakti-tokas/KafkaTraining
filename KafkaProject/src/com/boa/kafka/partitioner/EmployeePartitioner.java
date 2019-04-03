package com.boa.kafka.partitioner;

import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import com.boa.kafka.domain.Employee;

public class EmployeePartitioner implements Partitioner {

	@Override
	public void configure(Map<String, ?> arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

	@Override
	public int partition(String topicName, Object key, byte[] keyBytes, 
			Object value, byte[] valueBytes, Cluster cluster) {
		// TODO Auto-generated method stub
		int partition = 0;
		Employee employee = (Employee) value;
		
		if(employee.getDesignation().equals("Accountant")) {
			partition = 0;
					
		} else if(employee.getDesignation().equals("Developer")) {
			partition = 1;
					
		} else if(employee.getDesignation().equals("Architect")) {
			partition = 2;
					
		} else {
			partition = 3;
					
		}
		
		return partition;
	}

}
