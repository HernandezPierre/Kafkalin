package eu.nyuu.courses.utils;

import eu.nyuu.courses.model.AggregateTweet;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Class who wrap Kafka interactive queries to handle sentiments by tweet and types
 */
public class TweetQuery {
    private String key;
    private AggregateTweet tweetSentiment;

    public TweetQuery(String key, AggregateTweet aggTweet) {
        this.key = key;
        this.tweetSentiment = aggTweet;
    }

    public TweetQuery() {}

    public String getKey() { return key; }

    public void setKey(String key) { this.key = key; }

    public AggregateTweet getTweetSentiment() { return tweetSentiment; }

    public void setTweetSentiment(AggregateTweet aggTweet) { this.tweetSentiment = aggTweet; }

    /**
     * Get a list of sentiment by user
     * @param streams The Kafka stream to use
     * @return A list of sentiment by user
     * @throws Exception
     */
    public static List<TweetQuery> sentimentsByUser(KafkaStreams streams) throws Exception {
        if (streams.state() != KafkaStreams.State.RUNNING) throw new Exception("KafkaStreams not running");
        List<TweetQuery> sentimentsByUser = new ArrayList<>();
        ReadOnlyKeyValueStore<String,AggregateTweet> aggregateTweetStore =
            streams.store("sentiment-by-user-table-store-group2", QueryableStoreTypes.keyValueStore());
        KeyValueIterator<String, AggregateTweet> userSentimentIterator = aggregateTweetStore.all();
        while (userSentimentIterator.hasNext()) {
            KeyValue<String, AggregateTweet> next = userSentimentIterator.next();
            sentimentsByUser.add(new TweetQuery(next.key, next.value));
        }
        userSentimentIterator.close();
        return sentimentsByUser;
    }

    /**
     * Get a list of sentiment by year
     * @param streams The Kafka stream to use
     * @param byUser If we group by user too
     * @return A list of sentiment by year
     * @throws Exception
     */
    public static List<TweetQuery> sentimentsByYear(KafkaStreams streams, boolean byUser) throws Exception {
        if (streams.state() != KafkaStreams.State.RUNNING) throw new Exception("KafkaStreams not running");
        String storeName;
        if (byUser) storeName = "sentiment-by-year-table-store-group2";
        else storeName = "sentiment-by-year-user-table-store-group2";
        return sentimentByDate(streams, storeName, 365L);
    }

    /**
     * Get a list of sentiment by month
     * @param streams The Kafka stream to use
     * @param byUser If we group by user too
     * @return A list of sentiment by motn
     * @throws Exception
     */
    public static List<TweetQuery> sentimentsByMonth(KafkaStreams streams, boolean byUser) throws Exception {
        if (streams.state() != KafkaStreams.State.RUNNING) throw new Exception("KafkaStreams not running");
        String storeName;
        if (byUser) storeName = "sentiment-by-month-table-store-group2";
        else storeName = "sentiment-by-month-user-table-store-group2";
        return sentimentByDate(streams, storeName, 30L);
    }

    /**
     * Get a list of sentiment by day
     * @param streams The Kafka stream to use
     * @param byUser If we group by user too
     * @return A list of sentiment by day
     * @throws Exception
     */
    public static List<TweetQuery> sentimentsByDay(KafkaStreams streams, boolean byUser) throws Exception {
        if (streams.state() != KafkaStreams.State.RUNNING) throw new Exception("KafkaStreams not running");
        String storeName;
        if (byUser) storeName = "sentiment-by-day-table-store-group2";
        else storeName = "sentiment-by-day-user-table-store-group2";
        return sentimentByDate(streams, storeName, 1L);
    }

    /**
     * Get a list of sentiments for a date range
     * @param streams The Kafka stream to use
     * @param storeName The store to dig in
     * @param duration The range from now for the date
     * @return A list of sentiments for a date range
     */
    private static List<TweetQuery> sentimentByDate(KafkaStreams streams, String storeName, Long duration) {
        List<TweetQuery> sentimentsByDate = new ArrayList<>();
        ReadOnlyWindowStore<String,AggregateTweet> aggregateDateTweetStore = streams.store(storeName, QueryableStoreTypes.windowStore());
        Instant now = Instant.now();
        Instant lastDate = now.minus(Duration.ofDays(duration));
        KeyValueIterator<Windowed<String>, AggregateTweet> sentimentByDateIterator = aggregateDateTweetStore.fetchAll(lastDate, now);
        while (sentimentByDateIterator.hasNext()) {
            KeyValue<Windowed<String>, AggregateTweet> next = sentimentByDateIterator.next();
            sentimentsByDate.add(new TweetQuery(next.key.toString(), next.value));
        }
        sentimentByDateIterator.close();
        return sentimentsByDate;
    }
}
