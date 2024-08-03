package sample.kafka.beans;

import java.io.Serializable;

public class JoinedRecord implements Serializable {
    private String val1;    // from stream1
    private String val2;    // from stream2

    // No-argument constructor
    public JoinedRecord() {
    }

    // Parameterized constructor
    public JoinedRecord(String val1, String val2) {
        this.val1 = val1;
        this.val2 = val2;
    }

    // Getter for val1
    public String getVal1() {
        return val1;
    }

    // Setter for val1
    public void setVal1(String val1) {
        this.val1 = val1;
    }

    // Getter for val2
    public String getVal2() {
        return val2;
    }

    // Setter for val2
    public void setVal2(String val2) {
        this.val2 = val2;
    }

    @Override
    public String toString() {
        return "JoinedRecord{" +
                "val1='" + val1 + '\'' +
                ", val2='" + val2 + '\'' +
                '}';
    }
}
