package mqz.connector;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * @author maqingze
 * @version v1.0
 * @date 2019/3/19 15:31
 */
public class KafkaStreamCount {


    private final static String SOURCE_TOPIC = "source";
    private final static String SINK_TOPIC = "sink";
    private final static String GROUP_ID = "group1";
    private final static String METADATA_BROKER_LIST = "rdpe1:19092";
    private final static String ZOOKEEPER_CONNECT = "172.16.26.13:2181/kafka211";

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

        Table table = tEnv.sqlQuery("select count(id) from sourceTable group by id ");
        DataStream<Tuple2<Boolean, Row>> rowDataStream = tEnv.toRetractStream(table, Row.class);
        rowDataStream.print();
        env.execute(KafkaStreamCount.class.getCanonicalName());
    }
}
