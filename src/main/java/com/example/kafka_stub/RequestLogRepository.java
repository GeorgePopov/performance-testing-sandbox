package com.example.kafka_stub;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository // 1
public interface RequestLogRepository extends JpaRepository<RequestLog, String> { // 2
}
