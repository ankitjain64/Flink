package DataType;

public class Output {
    private Long count;
    private String type;

    public Output(String type, Long count){
        this.type=type;
        this.count=count;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return "Count=" + count +
                ", Type='" + type ;

    }
}
