package ops;

import org.apache.flink.api.java.tuple.Tuple3;

public class AggregateOp implements org.apache.flink.api.common.functions.ReduceFunction<Tuple3<Long, String, Long>> {
    @Override
    public Tuple3<Long, String, Long> reduce(Tuple3<Long, String,
            Long> rowOne, Tuple3<Long, String, Long> rowTwo) throws Exception {
        return new Tuple3<>(rowOne.f0, rowOne.f1, rowOne.f2 + rowTwo.f2);
    }
}