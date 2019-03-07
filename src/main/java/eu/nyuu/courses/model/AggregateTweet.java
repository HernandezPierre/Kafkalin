package eu.nyuu.courses.model;

/**
 * Aggregate sentiment for a group of tweet
 */
public class AggregateTweet {
    private Long veryNegative;
    private Long negative;
    private Long neutral;
    private Long positive;
    private Long veryPositive;

    public AggregateTweet() { }

    public AggregateTweet(Long veryNegative, Long negative, Long neutral, Long positive, Long veryPositive) {
        this.veryNegative = veryNegative;
        this.negative = negative;
        this.neutral = neutral;
        this.positive = positive;
        this.veryPositive = veryPositive;
    }

    public Long getVeryNegative() {
        return veryNegative;
    }

    public Long getNegative() {
        return negative;
    }

    public Long getNeutral() {
        return neutral;
    }

    public Long getPositive() {
        return positive;
    }

    public Long getVeryPositive() {
        return veryPositive;
    }

    public void setVeryNegative(Long veryNegative) { this.veryNegative = veryNegative; }

    public void setNegative(Long negative) { this.negative = negative; }

    public void setNeutral(Long neutral) { this.neutral = neutral; }

    public void setPositive(Long positive) { this.positive = positive; }

    public void setVeryPositive(Long veryPositive) { this.veryPositive = veryPositive; }

    /**
     * Add a sentiment to an existing `AggregateTweet`
     * @param sent The sentiment to add
     * @param aggTweet The AggregateTweet to aggregate on
     * @return The new aggregated tweet
     */
    public static AggregateTweet addSent(String sent, AggregateTweet aggTweet) {
        switch (sent) {
            case "VERY_NEGATIVE":
                return new AggregateTweet(aggTweet.veryNegative + 1L, aggTweet.negative,
                        aggTweet.neutral, aggTweet.positive, aggTweet.veryPositive);
            case "NEGATIVE":
                return new AggregateTweet(aggTweet.veryNegative, aggTweet.negative + 1L,
                        aggTweet.neutral, aggTweet.positive, aggTweet.veryPositive);
            case "NEUTRAL":
                return new AggregateTweet(aggTweet.veryNegative, aggTweet.negative,
                        aggTweet.neutral + 1L, aggTweet.positive, aggTweet.veryPositive);
            case "POSITIVE":
                return new AggregateTweet(aggTweet.veryNegative, aggTweet.negative,
                        aggTweet.neutral, aggTweet.positive + 1L, aggTweet.veryPositive);
            case "VERY_POSITIVE":
                return new AggregateTweet(aggTweet.veryNegative, aggTweet.negative,
                        aggTweet.neutral, aggTweet.positive, aggTweet.veryPositive + 1L);
            default:
                return new AggregateTweet(0L, 0L, 0L, 0L, 0L);
        }
    }
}
