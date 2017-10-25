package ops;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

public class AggregateApplyFunction implements WindowFunction<Tuple3<Long, String, Long>,
        Tuple3<TimeWindow, String, Long>, Tuple, TimeWindow> {
    @Override
    public void apply(Tuple tuple, TimeWindow window,
                      Iterable<Tuple3<Long, String, Long>> input,
                      Collector<Tuple3<TimeWindow, String, Long>> out) throws Exception {
        if (input == null) {
            return;
        }
        Map<String, Long> typeVsLong = new HashMap<>();
        for (Tuple3<Long, String, Long> row : input) {
            Long existing = typeVsLong.get(row.f1);
            if (existing == null) {
                existing = 0L;
            }
            typeVsLong.put(row.f1, existing + 1);
        }
        for (Map.Entry<String, Long> entry : typeVsLong.entrySet()) {
            String type = entry.getKey();
            Long value = entry.getValue();
            out.collect(new Tuple3<>(window, type, value));
        }
    }
}
