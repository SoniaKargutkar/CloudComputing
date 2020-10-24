import java.io.Serializable;

public class KeyValuePair implements Serializable
{
    private String key;
    private String value;
    private int length;
    private String flag;
    private String expdate;

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public int getLength() { return length; }

    public void setLength(int length) { this.length = length; }

    public String getFlag() {
        return flag;
    }

    public String getExpdate() {
        return expdate;
    }

    public void setExpdate(String expdate) {
        this.expdate = expdate;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }
}
