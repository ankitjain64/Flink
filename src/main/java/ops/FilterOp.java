package ops;

import DataType.Output;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class FilterOp implements org.apache.flink.api.common.functions.FilterFunction<Tuple2<TimeWindow, Output>> {
    @Override
    public boolean filter(Tuple2<TimeWindow, Output> row) throws Exception {
        return Long.compare(row.f1.getCount(), 100L) > 0;
    }
}
