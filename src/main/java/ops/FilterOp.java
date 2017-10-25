package ops;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class FilterOp implements org.apache.flink.api.common.functions.FilterFunction<Tuple3<TimeWindow, String, Long>> {
    @Override
    public boolean filter(Tuple3<TimeWindow, String, Long> row) throws Exception {
        return Long.compare(row.f2, 100L) > 0;
    }
}
