package xuwei.tech.batch

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.ExecutionEnvironment

object MyBatchWordCountScala {
  def main(args: Array[String]): Unit = {
    var inputPath = "D://tmp//file"
    var outputPath = "D://tmp//result"

    var env = ExecutionEnvironment.getExecutionEnvironment
    var text = env.readTextFile(inputPath)
    import org.apache.flink.api.scala._
    var counts = text.flatMap(_.toLowerCase.split("\\W+"))
      .filter(_.nonEmpty)
      .map((_, 1))
      .groupBy(0)
      .sum(1)
      .setParallelism(1)

    counts.writeAsCsv(outputPath, "\n", " ");
    env.execute("my batch word count")

  }

}
