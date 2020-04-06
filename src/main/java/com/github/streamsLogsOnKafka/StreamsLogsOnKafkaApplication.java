package com.github.streamsLogsOnKafka;

import com.github.streamsLogsOnKafka.models.Timer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;


@SpringBootApplication
public class StreamsLogsOnKafkaApplication {

	public static void main(String[] args) throws InterruptedException {
		SpringApplication.run(StreamsLogsOnKafkaApplication.class, args);
		Timer timer = new Timer();
		timer.log();
	}

}
