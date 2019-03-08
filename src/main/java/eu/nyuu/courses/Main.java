package eu.nyuu.courses;

import eu.nyuu.courses.model.AggregateTweet;
import eu.nyuu.courses.model.TweetEvent;
import eu.nyuu.courses.serdes.SerdeFactory;
import eu.nyuu.courses.utils.TweetQuery;
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

        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "tweet-stream-group-2-bis");

        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "tweet-stream-group-2-client-bis");
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

        KGroupedStream<String, String> hashtagsByKey = tweetStream
                .flatMap((k, v) -> {
                    List<KeyValue<String, String>> result = new LinkedList<>();
                    for(String s: v.findAllHashtags()) result.add(KeyValue.pair(s, v.getId()));
                    return result;
                }).groupByKey();

        // Get most popular hashtags for the last month
        hashtagsByKey
                .windowedBy(TimeWindows.of(Duration.ofDays(30L)))
                .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("hashtags-by-month-table-store-group2"))
                .filter((k,v) -> v > 5);

        // Get most popular hashtags for the last day
        hashtagsByKey.windowedBy(TimeWindows.of(Duration.ofDays(1L)))
                .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("hashtags-by-day-table-store-group2"))
                .filter((k,v) -> v > 5);


        final KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), streamsConfiguration);

        streams.cleanUp();
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        try { Thread.sleep(Duration.ofMinutes(1).toMillis()); }
        catch (Exception e) { System.out.println("Oh no it's broken ! SO LOLOLOLOLOLOLOLOL !"); }

        while (true) {
            try {
                System.out.println("Sentiment by users !");
                List<TweetQuery> sentimentsByUser = TweetQuery.sentimentsByUser(streams);
                for (int i = 0; i < sentimentsByUser.size(); i++) {
                    System.out.println(String.format("{user: %s, sentiment: {very_negative: %d, negative: %d, " +
                        "neutral: %d, positive: %d, very_positive: %d}}",
                        sentimentsByUser.get(i).getKey(),
                        sentimentsByUser.get(i).getTweetSentiment().getVeryNegative(),
                        sentimentsByUser.get(i).getTweetSentiment().getNegative(),
                        sentimentsByUser.get(i).getTweetSentiment().getNeutral(),
                        sentimentsByUser.get(i).getTweetSentiment().getPositive(),
                        sentimentsByUser.get(i).getTweetSentiment().getVeryPositive()));
                }
            } catch (Exception e) { System.out.println(e.getMessage()); }

            try {
                System.out.println("Sentiment by year !");
                List<TweetQuery> sentimentsByYear = TweetQuery.sentimentsByYear(streams, false);
                for (int i = 0; i < sentimentsByYear.size(); i++) {
                    System.out.println(String.format("{year: %s, sentiment: {very_negative: %d, negative: %d, " +
                                    "neutral: %d, positive: %d, very_positive: %d}}",
                            sentimentsByYear.get(i).getKey(),
                            sentimentsByYear.get(i).getTweetSentiment().getVeryNegative(),
                            sentimentsByYear.get(i).getTweetSentiment().getNegative(),
                            sentimentsByYear.get(i).getTweetSentiment().getNeutral(),
                            sentimentsByYear.get(i).getTweetSentiment().getPositive(),
                            sentimentsByYear.get(i).getTweetSentiment().getVeryPositive()));
                }
            } catch (Exception e) { System.out.println(e.getMessage()); }

            try {
                System.out.println("Sentiment by month !");
                List<TweetQuery> sentimentsByMonth = TweetQuery.sentimentsByMonth(streams, false);
                for (int i = 0; i < sentimentsByMonth.size(); i++) {
                    System.out.println(String.format("{month: %s, sentiment: {very_negative: %d, negative: %d, " +
                                    "neutral: %d, positive: %d, very_positive: %d}}",
                            sentimentsByMonth.get(i).getKey(),
                            sentimentsByMonth.get(i).getTweetSentiment().getVeryNegative(),
                            sentimentsByMonth.get(i).getTweetSentiment().getNegative(),
                            sentimentsByMonth.get(i).getTweetSentiment().getNeutral(),
                            sentimentsByMonth.get(i).getTweetSentiment().getPositive(),
                            sentimentsByMonth.get(i).getTweetSentiment().getVeryPositive()));
                }
            } catch (Exception e) { System.out.println(e.getMessage()); }

            try {
                System.out.println("Sentiment by day !");
                List<TweetQuery> sentimentsByDay = TweetQuery.sentimentsByDay(streams, false);
                for (int i = 0; i < sentimentsByDay.size(); i++) {
                    System.out.println(String.format("{day: %s, sentiment: {very_negative: %d, negative: %d, " +
                                    "neutral: %d, positive: %d, very_positive: %d}}",
                            sentimentsByDay.get(i).getKey(),
                            sentimentsByDay.get(i).getTweetSentiment().getVeryNegative(),
                            sentimentsByDay.get(i).getTweetSentiment().getNegative(),
                            sentimentsByDay.get(i).getTweetSentiment().getNeutral(),
                            sentimentsByDay.get(i).getTweetSentiment().getPositive(),
                            sentimentsByDay.get(i).getTweetSentiment().getVeryPositive()));
                }
            } catch (Exception e) { System.out.println(e.getMessage()); }

            try {
                System.out.println("Sentiment by year and users !");
                List<TweetQuery> sentimentsByYearAndUser = TweetQuery.sentimentsByYear(streams, true);
                for (int i = 0; i < sentimentsByYearAndUser.size(); i++) {
                    System.out.println(String.format("{user: %s, sentiment: {very_negative: %d, negative: %d, " +
                                    "neutral: %d, positive: %d, very_positive: %d}}",
                            sentimentsByYearAndUser.get(i).getKey(),
                            sentimentsByYearAndUser.get(i).getTweetSentiment().getVeryNegative(),
                            sentimentsByYearAndUser.get(i).getTweetSentiment().getNegative(),
                            sentimentsByYearAndUser.get(i).getTweetSentiment().getNeutral(),
                            sentimentsByYearAndUser.get(i).getTweetSentiment().getPositive(),
                            sentimentsByYearAndUser.get(i).getTweetSentiment().getVeryPositive()));
                }
            } catch (Exception e) { System.out.println(e.getMessage()); }

            try {
                System.out.println("Sentiment by month and users !");
                List<TweetQuery> sentimentsByMonthAndUser = TweetQuery.sentimentsByMonth(streams, true);
                for (int i = 0; i < sentimentsByMonthAndUser.size(); i++) {
                    System.out.println(String.format("{user: %s, sentiment: {very_negative: %d, negative: %d, " +
                                    "neutral: %d, positive: %d, very_positive: %d}}",
                            sentimentsByMonthAndUser.get(i).getKey(),
                            sentimentsByMonthAndUser.get(i).getTweetSentiment().getVeryNegative(),
                            sentimentsByMonthAndUser.get(i).getTweetSentiment().getNegative(),
                            sentimentsByMonthAndUser.get(i).getTweetSentiment().getNeutral(),
                            sentimentsByMonthAndUser.get(i).getTweetSentiment().getPositive(),
                            sentimentsByMonthAndUser.get(i).getTweetSentiment().getVeryPositive()));
                }
            } catch (Exception e) { System.out.println(e.getMessage()); }

            try {
                System.out.println("Sentiment by day and users !");
                List<TweetQuery> sentimentsByDayAndUser = TweetQuery.sentimentsByDay(streams, true);
                for (int i = 0; i < sentimentsByDayAndUser.size(); i++) {
                    System.out.println(String.format("{user: %s, sentiment: {very_negative: %d, negative: %d, " +
                                    "neutral: %d, positive: %d, very_positive: %d}}",
                            sentimentsByDayAndUser.get(i).getKey(),
                            sentimentsByDayAndUser.get(i).getTweetSentiment().getVeryNegative(),
                            sentimentsByDayAndUser.get(i).getTweetSentiment().getNegative(),
                            sentimentsByDayAndUser.get(i).getTweetSentiment().getNeutral(),
                            sentimentsByDayAndUser.get(i).getTweetSentiment().getPositive(),
                            sentimentsByDayAndUser.get(i).getTweetSentiment().getVeryPositive()));
                }
            } catch (Exception e) { System.out.println(e.getMessage()); }

            // Dumping all keys every minute
            try { Thread.sleep(Duration.ofMinutes(1).toMillis()); }
            catch (Exception e) { System.out.println("Oh no it's broken ! SO LOLOLOLOLOLOLOLOL !"); }

        }
    }
}
