package eu.nyuu.courses.model;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.*;

/**
 * A tweet event from Kafka
 */
public class TweetEvent {
    private String id;
    private String nick;
    private String body;
    private String timestamp;

    public TweetEvent() { }

    public TweetEvent(String id, String nick, String body, String timestamp) {
        this.id = id;
        this.nick = nick;
        this.body = body;
        this.timestamp = timestamp;
    }

    public String getId() {
        return id;
    }

    public String getNick() {
        return nick;
    }

    public String getBody() { return body.replaceAll("[^\\x00-\\x7F]", ""); }

    public String getTimestamp() {
        return timestamp;
    }

    public LocalDateTime getTimestampAsDate() { return LocalDateTime.parse(timestamp, DateTimeFormatter.ISO_OFFSET_DATE_TIME); }

    public void setId(String id) { this.id = id; }

    public void setNick(String nick) { this.nick = nick; }

    public void setBody(String body) { this.body = body; }

    public void setTimestamp(String timestamp) { this.timestamp = timestamp; }

    /**
     * Find all hashtags for a tweet
     * @return A list of hashtags for a tweet
     */
    public List<String> findAllHashtags() {
        List<String> hashtags = new ArrayList<String>();
        Pattern pattern = Pattern.compile(this.body);
        Matcher matcher = pattern.matcher("#(\\w+)");
        if (matcher.matches()) {
            for(int i=0; i <= matcher.groupCount(); i++)
                hashtags.add(matcher.group(i));
        }
        return hashtags;
    }
}
