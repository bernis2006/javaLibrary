package com.trim.kafkarest.storages;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

@Component
public class MessageStorage {

	private List<String> storage = new ArrayList<>();

	public void put(String message) {
		storage.add(message);
	}

	public String toString() {
		StringBuffer buffer = new StringBuffer();
		storage.forEach(msg -> buffer.append(msg).append("<br/>"));
		return buffer.toString();
	}
	
	public void clear(){
		storage.clear();
	}

}
