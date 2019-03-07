package eu.nyuu.courses;

import eu.nyuu.courses.model.AggregateTweet;
import eu.nyuu.courses.model.TweetEvent;
import eu.nyuu.courses.serdes.SerdeFactory;
import eu.nyuu.courses.utils.Utils;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.*;

import java.time.Duration;
import java.util.*;


public class Main {

    public static void main(final String[] args) {
        final String bootstrapServers = args.length > 0 ? args[0] : "51.15.90.153:9092";  // "localhost:29092";
        final Properties streamsConfiguration = new Properties();

        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "tweet-stream-group-2");
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "tweet-stream-group-2-app-client");
        // Where to find Kafka broker(s).
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // Specify default (de)serializers for record keys and for record values.
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        final Serde<String> stringSerde = Serdes.String();
        final Map<String, Object> serdeProps = new HashMap<>();
        final Serde<TweetEvent> sensorEventSerde = SerdeFactory.createSerde(TweetEvent.class, serdeProps);

        final StreamsBuilder streamsBuilder = new StreamsBuilder();
        final KStream<String, TweetEvent> tweetStream = streamsBuilder
            .stream("tweets", Consumed.with(stringSerde, sensorEventSerde));

        // Get the distribution of sentiments by users
        tweetStream
            .map((key, tweetEvent) -> KeyValue.pair(tweetEvent.getNick(), Utils.sentenceToSentiment(tweetEvent.getBody())))
            .groupByKey()
            .aggregate(
                () -> new AggregateTweet(0L, 0L, 0L, 0L, 0L),
                (aggNick, newSent, aggTweet) -> AggregateTweet.addSent(newSent, aggTweet),
                Materialized.<String, AggregateTweet, KeyValueStore<Bytes, byte[]>>as("sentiment-by-user-table-store-group2")
                    .withValueSerde(SerdeFactory.createSerde(AggregateTweet.class, serdeProps)));

        // Get the distribution of sentiments by year
        tweetStream
            .map((key, tweetEvent) -> KeyValue.pair("YEAR", Utils.sentenceToSentiment(tweetEvent.getBody())))
            .groupByKey()
            .windowedBy(TimeWindows.of(Duration.ofDays(365L)))
            .aggregate(
                () -> new AggregateTweet(0L, 0L, 0L, 0L, 0L),
                (agg, newSent, aggTweet) -> AggregateTweet.addSent(newSent, aggTweet),
                Materialized.<String, AggregateTweet, WindowStore<Bytes, byte[]>>as("sentiment-by-year-table-store-group2")
                    .withValueSerde(SerdeFactory.createSerde(AggregateTweet.class, serdeProps)));

        // Get the distribution of sentiments by month
        tweetStream
            .map((key, tweetEvent) -> KeyValue.pair("MONTH", Utils.sentenceToSentiment(tweetEvent.getBody())))
            .groupByKey()
            .windowedBy(TimeWindows.of(Duration.ofDays(30L)))
            .aggregate(
                 () -> new AggregateTweet(0L, 0L, 0L, 0L, 0L),
                 (aggNick, newSent, aggTweet) -> AggregateTweet.addSent(newSent, aggTweet),
                 Materialized.<String, AggregateTweet, WindowStore<Bytes, byte[]>>as("sentiment-by-month-table-store-group2")
                     .withValueSerde(SerdeFactory.createSerde(AggregateTweet.class, serdeProps)));

        // Get the distribution of sentiments by day
        tweetStream
            .map((key, tweetEvent) -> KeyValue.pair("DAY", Utils.sentenceToSentiment(tweetEvent.getBody())))
            .groupByKey()
            .windowedBy(TimeWindows.of(Duration.ofDays(1L)))
            .aggregate(
                 () -> new AggregateTweet(0L, 0L, 0L, 0L, 0L),
                 (aggNick, newSent, aggTweet) -> AggregateTweet.addSent(newSent, aggTweet),
                 Materialized.<String, AggregateTweet, WindowStore<Bytes, byte[]>>as("sentiment-by-day-table-store-group2")
                     .withValueSerde(SerdeFactory.createSerde(AggregateTweet.class, serdeProps)));

        // Get the distribution of sentiments by users and year
        tweetStream
            .map((key, tweetEvent) -> KeyValue.pair(tweetEvent.getNick(), Utils.sentenceToSentiment(tweetEvent.getBody())))
            .groupByKey()
            .windowedBy(TimeWindows.of(Duration.ofDays(365L)))
            .aggregate(
                  () -> new AggregateTweet(0L, 0L, 0L, 0L, 0L),
                  (agg, newSent, aggTweet) -> AggregateTweet.addSent(newSent, aggTweet),
                  Materialized.<String, AggregateTweet, WindowStore<Bytes, byte[]>>as("sentiment-by-year-user-table-store-group2")
                      .withValueSerde(SerdeFactory.createSerde(AggregateTweet.class, serdeProps)));

        // Get the distribution of sentiments by users and month
        tweetStream
            .map((key, tweetEvent) -> KeyValue.pair(tweetEvent.getNick(), Utils.sentenceToSentiment(tweetEvent.getBody())))
            .groupByKey()
            .windowedBy(TimeWindows.of(Duration.ofDays(30L)))
            .aggregate(
                 () -> new AggregateTweet(0L, 0L, 0L, 0L, 0L),
                 (aggNick, newSent, aggTweet) -> AggregateTweet.addSent(newSent, aggTweet),
                 Materialized.<String, AggregateTweet, WindowStore<Bytes, byte[]>>as("sentiment-by-month-user-table-store-group2")
                     .withValueSerde(SerdeFactory.createSerde(AggregateTweet.class, serdeProps)));

        // Get the distribution of sentiments by users and day
        tweetStream
             .map((key, tweetEvent) -> KeyValue.pair(tweetEvent.getNick(), Utils.sentenceToSentiment(tweetEvent.getBody())))
             .groupByKey()
             .windowedBy(TimeWindows.of(Duration.ofDays(1L)))
             .aggregate(
                  () -> new AggregateTweet(0L, 0L, 0L, 0L, 0L),
                  (aggNick, newSent, aggTweet) -> AggregateTweet.addSent(newSent, aggTweet),
                  Materialized.<String, AggregateTweet, WindowStore<Bytes, byte[]>>as("sentiment-by-day-user-table-store-group2")
                      .withValueSerde(SerdeFactory.createSerde(AggregateTweet.class, serdeProps)));

        final KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), streamsConfiguration);

        streams.cleanUp();
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
