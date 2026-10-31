package dev.vortex.api;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * Live market data service (M8, ADR 0017): consumes Kafka "trades", keeps the last price and the
 * current 1-minute candles per symbol in Redis, and serves them over HTTP.
 *
 * The hot path of the platform. The engine only matches; everything here is derived from its
 * trades, and the lakehouse (bronze -> silver -> gold) stays the authoritative history.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class ApiApplication {

    public static void main(String[] args) {
        SpringApplication.run(ApiApplication.class, args);
    }
}
