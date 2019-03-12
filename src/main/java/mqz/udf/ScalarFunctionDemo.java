package mqz.udf;

import org.apache.flink.streaming.api.scala.DataStream;
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.scala.StreamTableEnvironment;

/**
 * @author maqingze
 * @version v1.0
 * @date 2019/3/6 10:37
 */
public class ScalarFunctionDemo {


    public static void main(String[] args) {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

        DataStream<String> stream = null;
        Table myTable = tEnv.fromDataStream(stream);
        myTable.printSchema();
        tEnv.registerTable("myTable",myTable);
        // register the function
        tEnv.registerFunction("hashCode", new HashCode());

        // use the function in Java Table API
//        table.select("string, string.hashCode(), hashCode(string)");

        // use the function in SQL API
        tEnv.sqlQuery("SELECT f0, HASHCODE(f0) FROM myTable");

        env.execute();
    }
}
