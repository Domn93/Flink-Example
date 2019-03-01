package xuwei.tech.streaming

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

object MySocketWindowWordCount {
  def main(args: Array[String]): Unit = {

    var port:Int = try {
      ParameterTool.fromArgs(args).getInt("port")
    }catch {
      case e:Exception =>{
        System.err.println("No set port")
      }
        9001
    }

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val hostname = "172.16.13.185"
    val text = env.socketTextStream(hostname,9001,'\n')

    import org.apache.flink.api.scala._
    val windowCounts = text.flatMap(line => line.split("\\s"))
      .map(w => WordWithCount(w,1))
      .keyBy("word")
      .timeWindow(Time.seconds(2),Time.seconds(1))
      .sum("count")

    windowCounts.print().setParallelism(1)
    env.execute("Socket window start count")
  }

  case class WordWithCount(word: String, count: Int)

}
