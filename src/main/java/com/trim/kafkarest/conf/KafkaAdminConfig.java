package com.trim.kafkarest.conf;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaAdminConfig {

    @Bean
    public AdminClient adminClient() {
        // Configuración de las propiedades de Kafka para el AdminClient
        Map<String, Object> config = new HashMap<>();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // Cambia la dirección según tu entorno
        config.put(AdminClientConfig.CLIENT_ID_CONFIG, "admin-client");

        return AdminClient.create(config);
    }
}
