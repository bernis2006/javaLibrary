package com.trim.kafkarest.storages;

import org.springframework.stereotype.Component;
import java.util.ArrayList;
import java.util.List;

@Component
public class MessageStorageConsumer {

    private final List<String> messages = new ArrayList<>();

    public void add(String message) {
        messages.add(message);
    }

    @Override
    public String toString() {
        return String.join(", ", messages);
    }
}
