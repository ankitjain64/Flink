import DataType.Output;
import ops.AggregateApplyFunction;
import ops.CreateInputView;
import ops.EventTimeAssigner;
import ops.FilterOp;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
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
                .assignTimestampsAndWatermarks(new EventTimeAssigner()).keyBy(1);

        TumblingEventTimeWindows windowAssigner = TumblingEventTimeWindows.of(seconds(60));
        WindowedStream<Tuple3<Long, String, Long>, Tuple, TimeWindow> streamWithoutDelay = keyedStream.window(windowAssigner);
        WindowedStream<Tuple3<Long, String, Long>, Tuple, TimeWindow> streamWithDelay = streamWithoutDelay.allowedLateness(seconds(allowedLateness));

        SingleOutputStreamOperator<Tuple2<TimeWindow, Output>>
                withDelayStream = streamWithDelay.apply(new AggregateApplyFunction()).filter(new FilterOp());

        withDelayStream.print();
        executionEnvironment.execute("PartCQuestion2_" + allowedLateness);
    }
}
