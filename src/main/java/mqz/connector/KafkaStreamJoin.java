package mqz.connector;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.types.Row;

/**
 * @author maqingze
 * @version v1.0
 * @date 2019/3/19 16:12
 */
public class KafkaStreamJoin {

    private final static String SOURCE_TOPIC = "source";
    private final static String SINK_TOPIC = "sink";
    private final static String GROUP_ID = "group1";
    private final static String METADATA_BROKER_LIST = "rdpe1:19092";
    private final static String ZOOKEEPER_CONNECT = "172.16.26.13:2181/kafka211";
    private final static String FILE_PATH = "D:\\Aupload\\flink-commfun\\file_source.csv";

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


        tEnv.connect(
            new FileSystem()
                .path(FILE_PATH)
        )
            .withFormat(
                new Csv()
                    .field("id", Types.LONG)    // required: ordered format fields
                    .field("product", Types.STRING)
                    .field("amount", Types.INT)
                    .fieldDelimiter("|")              // optional: string delimiter "," by default
                    .lineDelimiter("\n")              // optional: string delimiter "\n" by default
                    .ignoreFirstLine()                // optional: ignore the first line, by default it is not skipped
                    .ignoreParseErrors()              // optional: skip records with parse error instead of failing by default
            )
            .withSchema(
                new Schema()
                    .field("id", Types.LONG)
                    .field("product", Types.STRING)
                    .field("amount", Types.INT)
            )
            .inRetractMode()
            .registerTableSource("fileTable");

        Table table = tEnv.sqlQuery("select a.product,b.product,a.amount + b.amount from sourceTable as a left join fileTable as b on a.id = b.id");
//        Table table = tEnv.sqlQuery("select * from fileTable");
//        tEnv.toAppendStream(table,Row.class).print();
        tEnv.toRetractStream(table, Row.class).print();
        env.execute(KafkaStreamCount.class.getCanonicalName());
    }
}
