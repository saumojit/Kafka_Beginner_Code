package sample.kafka.beans;

import java.time.LocalDateTime;

public class Message {
    private String message;
    private boolean bot;
    private LocalDateTime timestamp;

    // Default constructor
    public Message() {
    }

    // Parameterized constructor
    public Message(String message, boolean bot, LocalDateTime timestamp) {
        this.message = message;
        this.bot = bot;
        this.timestamp = timestamp;
    }

    // Getter and Setter for message
    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    // Getter and Setter for bot
    public boolean isBot() {
        return bot;
    }

    public void setBot(boolean bot) {
        this.bot = bot;
    }

    // Getter and Setter for timestamp
    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Message{" +
                "message='" + message + '\'' +
                ", bot=" + bot +
                ", timestamp=" + timestamp +
                '}';
    }
}
