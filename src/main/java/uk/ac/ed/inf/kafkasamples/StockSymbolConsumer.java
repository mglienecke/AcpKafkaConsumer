package uk.ac.ed.inf.kafkasamples;



import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;

/**
 * a simple stock symbol consumer (generated values)
 */
public class StockSymbolConsumer {

    /**
     * launch the consumer
     * @param args
     * @throws IOException
     * @throws InterruptedException
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length != 1) {
            System.out.println("Please provide the configuration file path as a command line argument");
            System.exit(1);
        }

        var consumer = new StockSymbolConsumer();
        consumer.process(args[0]);
    }


    // We'll reuse this function to load properties from the Consumer as well
    public static Properties loadConfig(final String configFile) throws IOException {
        if (!Files.exists(Paths.get(configFile))) {
            throw new IOException(configFile + " not found.");
        }
        final Properties cfg = new Properties();
        try (InputStream inputStream = new FileInputStream(configFile)) {
            cfg.load(inputStream);
        }
        return cfg;
    }

    public final String StockSymbolsConfig = "stock.symbols";
    public final String KafkaTopicConfig = "kafka.topic";

    /**
     *
     * @param configFileName
     * @throws IOException
     * @throws InterruptedException
     */
    private void process(String configFileName) throws IOException, InterruptedException {
        Properties kafkaPros = StockSymbolConsumer.loadConfig(configFileName);

        var consumer = new KafkaConsumer(kafkaPros);

        int recordCount = 0;
        final String topic = kafkaPros.getProperty(KafkaTopicConfig);
        HashMap<String, Double> currentSymbolValueMap = new HashMap<>();

        String[] symbols = ((String) kafkaPros.get(StockSymbolsConfig)).split(",");
        for (var symbol : symbols) {
            currentSymbolValueMap.put(symbol, Double.NaN);
        }

        consumer.subscribe(Collections.singletonList(topic));

        int iteration = 1;

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records){
                if (currentSymbolValueMap.containsKey(record.key())){
                    currentSymbolValueMap.put(record.key(), Double.parseDouble(record.value()));

                    System.out.println(String.format("[%s] %s: %s %s %s %s", record.topic(), record.key(), record.value(), record.partition(), record.offset(), record.timestamp()));
                    recordCount++;
                } else {
                    System.out.println(String.format("Unknown symbol: %s with value: %s encountered", record.key(), record.value()));
                }
            }

            // Thread.sleep(100);
            System.out.println("Iteration: " + iteration + " - " + recordCount + " records received Kafka");
            iteration += 1;
        }
    }
}

