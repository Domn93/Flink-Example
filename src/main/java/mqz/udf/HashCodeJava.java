package mqz.udf;

import org.apache.flink.table.functions.ScalarFunction;

/**
 * @author maqingze
 * @version v1.0
 * @date 2019/3/6 10:37
 */
public class HashCodeJava extends ScalarFunction {
    private int factor = 12;

    public HashCodeJava(int factor) {
        this.factor = factor;
    }

    public int eval(String s) {
        return s.hashCode() * factor;
    }

}


