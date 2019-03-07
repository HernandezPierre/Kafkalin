package eu.nyuu.courses;

import edu.stanford.nlp.simple.*;

public class Utils {

    /**
     * Get the general sentiment of a text
     * @param text
     *     The text to retrieve the sentiment
     * @return
     *     The the sentiment of the text
     *     (NEGATIVE/NEUTRAL/POSITIVE/VERY_NEGATIVE/VERY_POSITIVE)
     */
    public static String sentenceToSentiment(String text) {
        Sentence sent = new Sentence(text);
        SentimentClass sentiment = sent.sentiment();
        return sentiment.toString();
    }
}
