import ops.AggregateApplyFunction;
import ops.CreateInputView;
import ops.EventTimeAssigner;
import ops.FilterOp;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import static org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.getExecutionEnvironment;
import static org.apache.flink.streaming.api.windowing.time.Time.seconds;

public class PartCQuestion2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = getExecutionEnvironment();
        DataSourceBuilder.DataSource fileDataSource = DataSourceBuilder.createDataSource(args[0]);
        int allowedLateness = Integer.parseInt(args[1]);
        DataStreamSource<Tuple4<Integer, Integer, Long, String>> inputFileSourceStream = executionEnvironment.addSource(fileDataSource, "inputFileSource");
        executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        KeyedStream<Tuple3<Long, String, Long>, Tuple> keyedStream = inputFileSourceStream.map(new CreateInputView())
                .assignTimestampsAndWatermarks(new EventTimeAssigner()).keyBy(3);

        TumblingEventTimeWindows windowAssigner = TumblingEventTimeWindows.of(seconds(60), seconds(60));
        WindowedStream<Tuple3<Long, String, Long>, Tuple, TimeWindow> streamWithoutDelay = keyedStream.window(windowAssigner);
        WindowedStream<Tuple3<Long, String, Long>, Tuple, TimeWindow> streamWithDelay = streamWithoutDelay.allowedLateness(seconds(allowedLateness));
        SingleOutputStreamOperator<Tuple3<TimeWindow, String, Long>>
                withoutDelayStream = streamWithoutDelay.apply(new AggregateApplyFunction()).filter(new FilterOp());
        SingleOutputStreamOperator<Tuple3<TimeWindow, String, Long>>
                withDelayStream = streamWithDelay.apply(new AggregateApplyFunction()).filter(new FilterOp());
        DataStream<Integer> computedStream = withDelayStream.join(withoutDelayStream)
                .where(new JoinKeySelector()).equalTo(new JoinKeySelector())
                .window(windowAssigner).apply(new CustomJoinFunction());
        SingleOutputStreamOperator<Integer> sum = computedStream.keyBy(0).sum(0);
        sum.print();
        executionEnvironment.execute("PartCQuestion2_" + allowedLateness);
    }

    private static class JoinKeySelector implements
            KeySelector<Tuple3<TimeWindow, String, Long>, String> {
        @Override
        public String getKey(Tuple3<TimeWindow, String, Long>
                                     tuple) throws Exception {
            return tuple.f0.toString() + "_" + tuple.f1 + "_" + tuple.f2;
        }
    }

    private static class CustomJoinFunction implements
            JoinFunction<Tuple3<TimeWindow, String, Long>, Tuple3<TimeWindow,
                    String, Long>, Integer> {
        @Override
        public Integer join(Tuple3<TimeWindow, String, Long>
                                    inputOne, Tuple3<TimeWindow,
                String, Long> inputTwo) throws Exception {
            return 1;
        }
    }
}
