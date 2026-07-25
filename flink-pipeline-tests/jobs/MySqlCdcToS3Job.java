package demo;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;

/**
 * MySQL binlog (CDC via Debezium) -> S3. Each change (snapshot read r / insert c
 * / update u / delete d) is emitted as an append-only Debezium JSON line and
 * written to s3a://. Checkpoints/savepoints (incl. the binlog offset) go to S3.
 */
public class MySqlCdcToS3Job {
  private static java.util.Properties mkProps() {
    java.util.Properties p = new java.util.Properties();
    p.setProperty("decimal.handling.mode", "string"); // human-readable DECIMAL
    return p;
  }

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.enableCheckpointing(5000);
    env.setParallelism(1);

    MySqlSource<String> source = MySqlSource.<String>builder()
        .hostname("127.0.0.1").port(3306)
        .databaseList("inventory").tableList("inventory.orders")
        .username("flinkcdc").password("flinkpw")
        .startupOptions(StartupOptions.initial())
        .debeziumProperties(mkProps())
        .deserializer(new JsonDebeziumDeserializationSchema())
        .build();

    DataStream<String> changes = env
        .fromSource(source, WatermarkStrategy.noWatermarks(), "mysql-cdc-source")
        .uid("mysql-cdc-source");
    changes.print();

    FileSink<String> sink = FileSink
        .forRowFormat(new Path("s3a://flink-state/mysql-output"), new SimpleStringEncoder<String>("UTF-8"))
        .withRollingPolicy(DefaultRollingPolicy.builder()
            .withRolloverInterval(Duration.ofSeconds(10))
            .withInactivityInterval(Duration.ofSeconds(10))
            .withMaxPartSize(MemorySize.ofMebiBytes(64)).build())
        .build();
    changes.sinkTo(sink).uid("s3-sink");

    env.execute("mysql-cdc-to-s3");
  }
}
