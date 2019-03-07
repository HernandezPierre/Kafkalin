package eu.nyuu.courses;

import eu.nyuu.courses.model.AggregateTweet;
import eu.nyuu.courses.model.TweetEvent;
import eu.nyuu.courses.model.HashtagsTweet;
import eu.nyuu.courses.serdes.SerdeFactory;
import eu.nyuu.courses.Utils;

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

import java.security.KeyPair;
import java.time.Duration;
import java.time.Instant;
import java.util.*;


public class Main {

    public static void main(final String[] args) {
        final String bootstrapServers = args.length > 0 ? args[0] : "51.15.90.153:9092";  // "localhost:29092";
        final Properties streamsConfiguration = new Properties();

        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "tweet-stream-group-2");

        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "tweet-stream-group-2-client");
        // Where to find Kafka broker(s).
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // Specify default (de)serializers for record keys and for record values.
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        final Serde<String> stringSerde = Serdes.String();
        final Map<String, Object> serdeProps = new HashMap<>();
        final Serde<TweetEvent> sensorEventSerde = SerdeFactory.createSerde(TweetEvent.class, serdeProps);
        final Serde<HashtagsTweet> HashtagsTweet = SerdeFactory.createSerde(HashtagsTweet.class, serdeProps);

        final StreamsBuilder streamsBuilder = new StreamsBuilder();
        final KStream<String, TweetEvent> tweetStream = streamsBuilder
            .stream("tweets", Consumed.with(stringSerde, sensorEventSerde));


        /////////
        final KStream<String, HashtagsTweet> hashtagsStream = streamsBuilder
                .stream("tweets", Consumed.with(stringSerde, HashtagsTweet));


        /////////

        tweetStream
            .map((key, tweetEvent) -> KeyValue.pair(tweetEvent.getNick(), Utils.sentenceToSentiment(tweetEvent.getBody())))
            .groupByKey()
            .aggregate(
                () -> new AggregateTweet(0L, 0L, 0L, 0L, 0L),
                (aggNick, newSent, aggTweet) -> AggregateTweet.addSent(newSent, aggTweet),
                Materialized.<String, AggregateTweet, KeyValueStore<Bytes, byte[]>>as("sentiment-by-user-table-store-group2")
                    .withValueSerde(SerdeFactory.createSerde(AggregateTweet.class, serdeProps)));

        tweetStream
            .map((key, tweetEvent) -> KeyValue.pair("YEAR", Utils.sentenceToSentiment(tweetEvent.getBody())))
            .groupByKey()
            .windowedBy(TimeWindows.of(Duration.ofDays(365L)))
            .aggregate(
                () -> new AggregateTweet(0L, 0L, 0L, 0L, 0L),
                (agg, newSent, aggTweet) -> AggregateTweet.addSent(newSent, aggTweet),
                    Materialized.<String, AggregateTweet, WindowStore<Bytes, byte[]>>as("sentiment-by-year-table-store-group2")
                            .withValueSerde(SerdeFactory.createSerde(AggregateTweet.class, serdeProps)));

        tweetStream
            .map((key, tweetEvent) -> KeyValue.pair("MONTH", Utils.sentenceToSentiment(tweetEvent.getBody())))
            .groupByKey()
            .windowedBy(TimeWindows.of(Duration.ofDays(30L)))
            .aggregate(
                 () -> new AggregateTweet(0L, 0L, 0L, 0L, 0L),
                 (aggNick, newSent, aggTweet) -> AggregateTweet.addSent(newSent, aggTweet),
                 Materialized.<String, AggregateTweet, WindowStore<Bytes, byte[]>>as("sentiment-by-month-table-store-group2")
                    .withValueSerde(SerdeFactory.createSerde(AggregateTweet.class, serdeProps)));

        tweetStream
            .map((key, tweetEvent) -> KeyValue.pair("DAY", Utils.sentenceToSentiment(tweetEvent.getBody())))
            .groupByKey()
            .windowedBy(TimeWindows.of(Duration.ofDays(1L)))
            .aggregate(
                 () -> new AggregateTweet(0L, 0L, 0L, 0L, 0L),
                 (aggNick, newSent, aggTweet) -> AggregateTweet.addSent(newSent, aggTweet),
                 Materialized.<String, AggregateTweet, WindowStore<Bytes, byte[]>>as("sentiment-by-day-table-store-group2")
                     .withValueSerde(SerdeFactory.createSerde(AggregateTweet.class, serdeProps)));

        tweetStream
            .map((key, tweetEvent) -> KeyValue.pair(tweetEvent.getNick(), Utils.sentenceToSentiment(tweetEvent.getBody())))
            .groupByKey()
            .windowedBy(TimeWindows.of(Duration.ofDays(365L)))
            .aggregate(
                  () -> new AggregateTweet(0L, 0L, 0L, 0L, 0L),
                  (agg, newSent, aggTweet) -> AggregateTweet.addSent(newSent, aggTweet),
                  Materialized.<String, AggregateTweet, WindowStore<Bytes, byte[]>>as("sentiment-by-year-user-table-store-group2")
                      .withValueSerde(SerdeFactory.createSerde(AggregateTweet.class, serdeProps)));
        tweetStream
            .map((key, tweetEvent) -> KeyValue.pair(tweetEvent.getNick(), Utils.sentenceToSentiment(tweetEvent.getBody())))
            .groupByKey()
            .windowedBy(TimeWindows.of(Duration.ofDays(30L)))
            .aggregate(
                 () -> new AggregateTweet(0L, 0L, 0L, 0L, 0L),
                 (aggNick, newSent, aggTweet) -> AggregateTweet.addSent(newSent, aggTweet),
                 Materialized.<String, AggregateTweet, WindowStore<Bytes, byte[]>>as("sentiment-by-month-user-table-store-group2")
                     .withValueSerde(SerdeFactory.createSerde(AggregateTweet.class, serdeProps)));

        tweetStream
             .map((key, tweetEvent) -> KeyValue.pair(tweetEvent.getNick(), Utils.sentenceToSentiment(tweetEvent.getBody())))
             .groupByKey()
             .windowedBy(TimeWindows.of(Duration.ofDays(1L)))
             .aggregate(
                  () -> new AggregateTweet(0L, 0L, 0L, 0L, 0L),
                  (aggNick, newSent, aggTweet) -> AggregateTweet.addSent(newSent, aggTweet),
                  Materialized.<String, AggregateTweet, WindowStore<Bytes, byte[]>>as("sentiment-by-day-user-table-store-group2")
                      .withValueSerde(SerdeFactory.createSerde(AggregateTweet.class, serdeProps)));

        KTable<Windowed<String>, Long> hashtagStream = tweetStream
                .flatMap((k, v) -> {
                    List<KeyValue<String, String>> result = new LinkedList<>();
                    for(String s: v.findAllHashtags()) {
                        result.add(KeyValue.pair(s, v.getId()));
                    }
                    return result;
                })
                .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofMinutes(5)))
                .count()
                .filter((k,v)-> v >5);


        final KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), streamsConfiguration);

        streams.cleanUp();
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        try { Thread.sleep(Duration.ofMinutes(1).toMillis()); }
        catch (Exception e) { System.out.println("Oh no it's broken ! SO LOLOLOLOLOLOLOLOL !"); }

        while (true) {
            if (streams.state() == KafkaStreams.State.RUNNING) {
                // Querying our local store
                ReadOnlyKeyValueStore<String,AggregateTweet> aggregateTweetStore =
                        streams.store("sentiment-by-user-table-store-group2",
                                QueryableStoreTypes.keyValueStore());
                ReadOnlyWindowStore<String,AggregateTweet> aggregateYearsTweetStore =
                        streams.store("sentiment-by-year-table-store-group2",
                                QueryableStoreTypes.windowStore());

                System.out.println("Sentiment by users !");
                KeyValueIterator<String, AggregateTweet> userSentimentIterator = aggregateTweetStore.all();
                while (userSentimentIterator.hasNext()) {
                    KeyValue<String, AggregateTweet> next = userSentimentIterator.next();
                    System.out.println(String.format("{user: %s, sentiment: {very_negative: %d, negative: %d, " +
                        "neutral: %d, positive: %d, very_positive: %d}}", next.key, next.value.getVeryNegative(),
                        next.value.getNegative(), next.value.getNeutral(), next.value.getPositive(),
                        next.value.getVeryPositive()));
                }
                userSentimentIterator.close();

                System.out.println("Sentiment by years !");
                // fetching all values for the last year in the window
                Instant now = Instant.now();
                Instant lastYear = now.minus(Duration.ofDays(365L));
                KeyValueIterator<Windowed<String>, AggregateTweet> sentimentByYearIterator = aggregateYearsTweetStore.fetchAll(lastYear, now);
                while (sentimentByYearIterator.hasNext()) {
                    KeyValue<Windowed<String>, AggregateTweet> next = sentimentByYearIterator.next();
                    System.out.println(String.format("{year: %s, sentiment: {very_negative: %d, negative: %d, " +
                                    "neutral: %d, positive: %d, very_positive: %d}}", next.key, next.value.getVeryNegative(),
                            next.value.getNegative(), next.value.getNeutral(), next.value.getPositive(),
                            next.value.getVeryPositive()));
                }
                // close the iterator to release resources
                sentimentByYearIterator.close();
            }

            // Dumping all keys every minute
            try { Thread.sleep(Duration.ofMinutes(1).toMillis()); }
            catch (Exception e) { System.out.println("Oh no it's broken ! SO LOLOLOLOLOLOLOLOL !"); }

        }
    }
}
