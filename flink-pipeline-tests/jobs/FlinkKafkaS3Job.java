package demo;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Kafka -> (keyed running count in ValueState) -> S3 FileSink.
 * Checkpoints/savepoints go to S3 (see flink-conf.yaml). The per-key count is
 * the state we prove survives a stop/restore-from-S3-savepoint cycle.
 */
public class FlinkKafkaS3Job {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.enableCheckpointing(5000);

    KafkaSource<String> source = KafkaSource.<String>builder()
        .setBootstrapServers("127.0.0.1:9092")
        .setTopics("events")
        .setGroupId("flink-demo")
        .setStartingOffsets(OffsetsInitializer.earliest())
        .setValueOnlyDeserializer(new SimpleStringSchema())
        .build();

    DataStream<String> counts = env
        .fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-events")
        .keyBy(v -> v.trim())
        .process(new KeyedProcessFunction<String, String, String>() {
          private transient ValueState<Long> countState;
          @Override public void open(Configuration p) {
            countState = getRuntimeContext().getState(new ValueStateDescriptor<>("count", Long.class));
          }
          @Override public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
            long c = (countState.value() == null ? 0L : countState.value()) + 1;
            countState.update(c);
            out.collect(ctx.getCurrentKey() + "=" + c);
          }
        })
        .name("stateful-counter").uid("stateful-counter");

    counts.print();

    FileSink<String> sink = FileSink
        .forRowFormat(new Path("s3a://flink-state/output"), new SimpleStringEncoder<String>("UTF-8"))
        .withRollingPolicy(DefaultRollingPolicy.builder()
            .withRolloverInterval(Duration.ofSeconds(10))
            .withInactivityInterval(Duration.ofSeconds(10))
            .withMaxPartSize(MemorySize.ofMebiBytes(64))
            .build())
        .build();
    counts.sinkTo(sink).name("s3-file-sink").uid("s3-file-sink");

    env.execute("kafka-to-s3-stateful");
  }
}
