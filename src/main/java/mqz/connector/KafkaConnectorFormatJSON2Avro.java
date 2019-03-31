package mqz.connector;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Avro;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;

/**
 * @author maqingze
 * @version v1.0
 * @date 2019/3/7 11:24
 */
public class KafkaConnectorFormatJSON2Avro {
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


//        DataStream<Row> rowDataStream = tEnv.toAppendStream(result, Row.class);
//
//        rowDataStream.print();

        tEnv.connect(
            new Kafka()
                .version("0.10")    // required: valid connector versions are
                //   "0.8", "0.9", "0.10", "0.11", and "universal"
                .topic(SINK_TOPIC)       // required: topic name from which the table is read
                // optional: connector specific properties
                .property("zookeeper.connect", ZOOKEEPER_CONNECT)
                .property("bootstrap.servers", METADATA_BROKER_LIST)
                .property("group.id", GROUP_ID)
                // optional: select a startup mode for Kafka offsets
                .startFromEarliest()
                .sinkPartitionerFixed()         // each Flink partition ends up in at-most one Kafka partition (default)
        ).withFormat(
            new Avro()
            .avroSchema("{\"namespace\": \"example.avro\",\n" +
                " \"type\": \"record\",\n" +
                " \"name\": \"User\",\n" +
                " \"fields\": [\n" +
                "     {\"name\": \"id\", \"type\": \"long\"},\n" +
                "     {\"name\": \"product\",  \"type\": \"string\"},\n" +
                "     {\"name\": \"amount\", \"type\": \"int\"}\n" +
                " ]\n" +
                "}")
        )
            .withSchema(
                new Schema()
                    .field("id", Types.LONG)
                    .field("product", Types.STRING)
                    .field("amount", Types.INT)
            )
            .inAppendMode()
            .registerTableSink("sinkTable");


        tEnv.sqlUpdate("insert into sinkTable select * from sourceTable  ");

        env.execute(" tesst kafka connector demo");

    }

}
