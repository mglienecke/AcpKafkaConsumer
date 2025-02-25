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
     * The entry point of the application.
     * This method initializes the StockSymbolConsumer and processes symbols
     * based on the provided configuration file.
     *
     * @param args Command-line arguments where the first argument must be the path to the configuration file.
     * @throws IOException If there is an issue reading the configuration file.
     * @throws InterruptedException If the thread executing the method is interrupted during execution.
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length != 1) {
            System.out.println("Please provide the configuration file path as a command line argument");
            System.exit(1);
        }

        // TODO: create a consumer and call the method to process the messages
    }


    /**
     * Loads configuration properties from a file.
     *
     * This method reads properties from the specified configuration file and
     * returns a {@link Properties} object containing the key-value pairs from the file.
     * If the file does not exist, an {@link IOException} is thrown.
     *
     * @param configFile The path to the configuration file to be loaded.
     * @return A {@link Properties} object containing the configuration key-value pairs.
     * @throws IOException If the specified configuration file cannot be found or read.
     */
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
     * Processes stock symbol data from a Kafka topic based on the provided configuration file.
     *
     * The method reads configurations, sets up a Kafka consumer, subscribes to the defined topic,
     * and continuously polls messages. It updates and logs the symbol values for known stock symbols based on the configuration.
     * Unknown symbols are also logged. The method runs in an infinite loop and periodically pauses for a short duration.
     *
     * @param configFileName The path to the configuration file that contains Kafka and stock symbol settings.
     * @throws IOException If there are issues reading the configuration file.
     * @throws InterruptedException If the thread executing the method is interrupted.
     */
    private void process(String configFileName) throws IOException, InterruptedException {
        Properties kafkaPros = StockSymbolConsumer.loadConfig(configFileName);

        // TODO: Create the consumer

        int recordCount = 0;
        final String topic = kafkaPros.getProperty(KafkaTopicConfig);
        HashMap<String, Double> currentSymbolValueMap = new HashMap<>();

        String[] symbols = ((String) kafkaPros.get(StockSymbolsConfig)).split(",");
        for (var symbol : symbols) {
            currentSymbolValueMap.put(symbol, Double.NaN);
        }

        // TODO: Subscribe to the topics you are interested in

        int iteration = 1;

        while (true) {
            // TODO: Poll for records with a duration of 100 msec
            ConsumerRecords<String, String> records = xxx );

            for (ConsumerRecord<String, String> record : records){
                // TODO: check if the symbol is in your list of acceptable symbols
                if (currentSymbolValueMap....){
                    currentSymbolValueMap.put(record.key(), Double.parseDouble(record.value()));

                    System.out.println(String.format("[%s] %s: %s %s %s %s", record.topic(), record.key(), record.value(), record.partition(), record.offset(), record.timestamp()));
                    recordCount++;
                } else {
                    System.out.println(String.format("Unknown symbol: %s with value: %s encountered", record.key(), record.value()));
                }
            }

            // TODO: have a nap for 500 msec
            System.out.println("Iteration: " + iteration + " - " + recordCount + " records received Kafka");
            iteration += 1;
        }
    }
}

