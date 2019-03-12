package mqz.connector;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.types.Row;

/**
 * @author maqingze
 * @version v1.0
 * @date 2019/3/7 11:24
 */
public class KafkaConnectorFormatJSON2CSV {
    private final static String SOURCE_TOPIC = "source";
    private final static String SINK_TOPIC = "sink";
    private final static String ZOOKEEPER_CONNECT = "hadoop003:2181,hadoop004:2181";
    private final static String GROUP_ID = "group1";
    private final static String METADATA_BROKER_LIST = "hadoop003:9092,hadoop004:9092";


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);
        tEnv.connect(
            new Kafka()
                .version("0.10")
                .topic(SOURCE_TOPIC)
                .startFromEarliest()
                .property("zookeeper.connect", ZOOKEEPER_CONNECT)
                .property("bootstrap.servers", METADATA_BROKER_LIST)
        )
            .withFormat(
//                new Csv()
//                    .field("id", Types.STRING)
//                    .field("product", Types.STRING)
//                    .field("amount", Types.STRING)
//                    .fieldDelimiter(",")              // optional: string delimiter "," by default
//                    .lineDelimiter("\n")              // optional: string delimiter "\n" by default
//                    .ignoreParseErrors()
            new Json()
                .schema(
                    org.apache.flink.table.api.Types.ROW(
                        new String[]{"id", "product", "amount"},
                        new TypeInformation[]{
                            org.apache.flink.table.api.Types.LONG()
                            , org.apache.flink.table.api.Types.STRING()
                            , org.apache.flink.table.api.Types.INT()
                        }))
                .failOnMissingField(true)   // optional: flag whether to fail if a field is missing or not, false by default
            )
            .withSchema(
                new Schema()
                    .field("id", Types.LONG)
                    .field("product", Types.STRING)
                    .field("amount", Types.INT)
            )
            .inAppendMode()
            .registerTableSource("sourceTable");

        Table result = tEnv.sqlQuery("select * from sourceTable ");


        DataStream<Row> rowDataStream = tEnv.toAppendStream(result, Row.class);

        rowDataStream.print();

//        TableConversions tableConversions = new TableConversions(result);
//        tableConversions.toAppendStream(Types.ROW()).print();


        CsvTableSink sink = new CsvTableSink(
            "D:\\Aupload\\flink\\sink.csv",                  // 输出路径
            "|",                   // 字段分隔符
            1,                     // 写入的文件个数
            FileSystem.WriteMode.OVERWRITE);  // 是否覆盖原文件 还有NO_OVERWRITE模式

        tEnv.registerTableSink(
            "csvOutputTable",
            new String[]{"f0", "f1", "f2"},
            new TypeInformation[]{Types.LONG, Types.STRING, Types.INT},
            sink);

        result.insertInto("csvOutputTable");

        env.execute(" tesst kafka connector demo");

    }

}
