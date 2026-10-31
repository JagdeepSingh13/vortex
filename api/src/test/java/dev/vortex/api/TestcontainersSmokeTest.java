package dev.vortex.api;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

// temporary: proves Testcontainers can start containers from inside the Maven build container
class TestcontainersSmokeTest {

    @Test
    void startsRedisAndKafka() {
        try (var redis = new GenericContainer<>(DockerImageName.parse("redis:8.8.2-alpine")).withExposedPorts(6379);
             var kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:4.3.1"))) {
            redis.start();
            kafka.start();
            assertThat(redis.isRunning()).isTrue();
            assertThat(kafka.getBootstrapServers()).isNotBlank();
        }
    }
}
