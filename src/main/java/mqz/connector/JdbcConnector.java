package mqz.connector;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author maqingze
 * @version v1.0
 * @date 2019/3/22 10:52
 */
public class JdbcConnector {
    private static final String DRIVER_CLASS = "com.mysql.jdbc.Driver";
    private static final String DB_URL = "jdbc:mysql://localhost:3306/test";
    private static final String USER_NAME = "root";
    private static final String USER_PASSWORD = "root";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);
        JDBCInputFormat inputBuilder = JDBCInputFormat.buildJDBCInputFormat()
            .setDrivername(DRIVER_CLASS)
            .setDBUrl(DB_URL)
//            .setUsername(USER_NAME)
//            .setPassword(USER_PASSWORD)
            .setQuery("SELECT * from jdbc_source")
            .setRowTypeInfo(new RowTypeInfo(
                BasicTypeInfo.INT_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO))
            .finish();


        DataStreamSource<Row> source = env.createInput(inputBuilder);
        tEnv.registerDataStream("jdbcTable",source,"id,s");
        source.print();
        Table table = tEnv.sqlQuery("select * from jdbcTable");
//        tEnv.toRetractStream(table,Row.class).print();

        JDBCAppendTableSink sink = JDBCAppendTableSink.builder()
            .setDrivername(DRIVER_CLASS)
            .setDBUrl(DB_URL)
            .setQuery("INSERT INTO jdbc_sink VALUES (?,?,?)")
            .setParameterTypes(
                new TypeInformation[]{
                    BasicTypeInfo.INT_TYPE_INFO,
                    BasicTypeInfo.STRING_TYPE_INFO,
                    BasicTypeInfo.STRING_TYPE_INFO
                }
            )
            .build();


//        sink.configure(
//            new String[] {"Hello"},
//            new TypeInformation<?>[] {Types.STRING, Types.INT, Types.LONG});

        sink.emitDataStream(source);
        env.execute();

    }
}
