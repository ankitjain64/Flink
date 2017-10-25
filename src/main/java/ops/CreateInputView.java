package ops;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

public class CreateInputView implements MapFunction<Tuple4<Integer, Integer, Long, String>, Tuple3<Long, String, Long>> {
    @Override
    public Tuple3<Long, String, Long> map(Tuple4<Integer, Integer,
            Long, String> input) throws Exception {
        return new Tuple3<>(input.f2, input.f3, 1L);
    }
}
