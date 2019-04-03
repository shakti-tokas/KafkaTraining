package com.kafka.partitioner;

import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

public class MessagePartitioner implements Partitioner {

	@Override
	public void configure(Map<String, ?> arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

	@Override
	public int partition(String topic, Object key, byte[] keyBytes, Object value, 
			byte[] valueBytes, Cluster cluster) {
		// TODO Auto-generated method stub
		int partition = 0;
		String keyValue = new String(keyBytes);
		
		if(keyValue.equals("msg-1")) {
			partition = 0;
			
		} else if(keyValue.equals("msg-2")) {
			partition = 1;
			
		} else {			
			partition = 2;
			
		}
		
		return partition;
	}

}
