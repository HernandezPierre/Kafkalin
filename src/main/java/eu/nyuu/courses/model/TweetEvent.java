package eu.nyuu.courses.model;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class TweetEvent {
    private String id;
    private String nick;
    private String body;
    private String timestamp;

    public String getId() {
        return id;
    }

    public String getNick() {
        return nick;
    }

    public String getBody() {
        return body;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public LocalDateTime getTimestampAsDate() {
        return LocalDateTime.parse(timestamp, DateTimeFormatter.ISO_OFFSET_DATE_TIME);
    }

}
