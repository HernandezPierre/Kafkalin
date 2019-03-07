package eu.nyuu.courses.model;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HashtagsTweet {
    private String id;
    private String nick;
    private String body;
    private String timestamp;
    private List<String> hashtags;
    private Long count;

    public HashtagsTweet() { }

    public HashtagsTweet(String id, String nick, String body, String timestamp, List<String> hashtags) {
        this.id = id;
        this.nick = nick;
        this.body = body;
        this.timestamp = timestamp;
        this.hashtags= hashtags;
        this.count = new Long(hashtags.size());
    }
    public List<String> getHashtags(){return hashtags;}

    public Long getCount(){return count;}

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

}


