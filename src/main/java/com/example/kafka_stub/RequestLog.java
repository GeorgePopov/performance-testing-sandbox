package com.example.kafka_stub;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;

@Entity
@Table(name = "requests_log", schema = "public")
@Data
public class RequestLog {

    @Id
    private String msguuid;

    private Boolean head;

    private Long timerq;
}
