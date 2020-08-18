import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Random;

public class LocalWordCount {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> dataStream = env
                .readTextFile("C:\\MyProgram\\Project\\FlinkStudy\\DemoWindowWordCount\\src\\main\\java\\test.txt")
//                .addSource(new Producer())

                .flatMap(new WindowWordCount.Splitter())
                .keyBy(0)
                .timeWindow(Time.seconds(5))
                .sum(1);

//        dataStream.print();

        dataStream.addSink(new Customer());

        env.execute("Window WordCount");
    }

}
class Customer extends RichSinkFunction<Tuple2<String, Integer>> {

    @Override
    public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
        System.out.println(value.f0+", "+value.f1);
//        System.out.println(context.currentProcessingTime());
//        System.out.println(context.currentWatermark());
//        System.out.println(context.timestamp());
        System.out.println();
    }
}
class Producer implements SourceFunction<String>{
    boolean isRun = true;
    @Override
    public void run(SourceContext<String> ctx) {
        while (isRun) {
            ctx.collect("Alice: Hello");
            ctx.collect("Bob: Hello");
            ctx.collect("Candy: Hi");
        }
    }

    @Override
    public void cancel() {

    }
}
