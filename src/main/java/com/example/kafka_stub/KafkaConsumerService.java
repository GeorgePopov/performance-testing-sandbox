package com.example.kafka_stub;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import java.time.Instant;

@Service
@Slf4j
public class KafkaConsumerService {

    private final RequestLogRepository repository;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public KafkaConsumerService(RequestLogRepository repository) {
        this.repository = repository;
    }

    @KafkaListener(topics = "performance-test-topic", concurrency = "3"
    )
    public void listen(String message) {
        try {

            long unixTime = Instant.now().getEpochSecond(); // Засекаем текущее UNIX-время (число секунд)

            log.info("[Read from Kafka] {}", message); // Выводим первый лог по ТЗ о вычитке

            // Парсим входящий JSON-текст
            JsonNode jsonNode = objectMapper.readTree(message);
            String msgUuid = jsonNode.get("msg_uuid").asText();
            boolean head = jsonNode.get("head").asBoolean();

            // Формируем запись для БД
            RequestLog entity = new RequestLog();
            entity.setMsguuid(msgUuid);
            entity.setHead(head);
            entity.setTimerq(unixTime); // Просто передаем число секунд напрямую

            repository.save(entity); // Сохраняем в PostgreSQL

            log.info("[Write to DB] { \"msgUuid\": \"{}\", \"head\": {}, \"timeRq\": \"{}\" }",
                    msgUuid, head, unixTime); // Выводим второй лог по ТЗ о записи в БД

        } catch (Exception e) {
            log.error("Error processing message: ", e);
        }
    }
}
