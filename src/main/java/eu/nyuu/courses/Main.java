package eu.nyuu.courses;

import eu.nyuu.courses.model.TweetEvent;
import eu.nyuu.courses.serdes.SerdeFactory;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.*;


public class Main {

    public static void main(final String[] args) {
        final String bootstrapServers = args.length > 0 ? args[0] : "163.172.145.138:9092";  // "localhost:29092";
        final Properties streamsConfiguration = new Properties();

        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "tweet-stream-group-2-app");
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "tweet-stream-group-2-app-client");
        // Where to find Kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // Specify default (de)serializers for record keys and for record values.
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        final Serde<String> stringSerde = Serdes.String();
        final Map<String, Object> serdeProps = new HashMap<>();
        final Serde<TweetEvent> sensorEventSerde = SerdeFactory.createSerde(TweetEvent.class, serdeProps);


        final StreamsBuilder streamsBuilder = new StreamsBuilder();
        final KStream<String, TweetEvent> tweetStream = streamsBuilder
            .stream("tweet", Consumed.with(stringSerde, sensorEventSerde));

        final KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), streamsConfiguration);

        streams.cleanUp();
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
