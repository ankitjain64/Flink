package ops;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

public class EventTimeAssigner implements AssignerWithPeriodicWatermarks<Tuple3<Long, String, Long>> {
    private Long timeStamp;
    private Long delay;

    public EventTimeAssigner() {
    }

    public EventTimeAssigner(Long delay) {
        this.delay = delay;
    }

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        if (timeStamp == null) {
            return null;
        }
        if (delay == null) {
            return new Watermark(timeStamp);
        }
        return new Watermark(timeStamp - delay);
    }

    @Override
    public long extractTimestamp(Tuple3<Long, String, Long> inputRow, long l) {
        long currentTime = inputRow.f0 * 1000;
        if (this.timeStamp == null) {
            this.timeStamp = currentTime;
        } else {
            this.timeStamp = Math.max(currentTime, this.timeStamp);
        }
        return currentTime;
    }
}
