package mqz.table

import org.apache.flink.api.scala._
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row
import org.junit.rules.TemporaryFolder
import org.junit.{Rule, Test}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class SqlOverviewITCase {

  @Test
  def testSelect(): Unit = {

    println("selectSQL------------------------华丽的分割线----------------------------")
    val selectSQL = "SELECT c_name, CONCAT(c_name, ' come ', c_desc) as desc  FROM customer_tab"
    procTimePrint(selectSQL)

    println("distinctSQL------------------------华丽的分割线----------------------------")

    val distinctSQL = "SELECT DISTINCT c_id FROM order_tab"
    procTimePrint(distinctSQL)

    println("whereSQL------------------------华丽的分割线----------------------------")

    val whereSQL = "SELECT c_id, c_name, c_desc FROM customer_tab WHERE c_id = 'c_001' OR c_id = 'c_003'"
    procTimePrint(whereSQL)

    println("whereInSQL------------------------华丽的分割线----------------------------")

    val whereInSQL = "SELECT c_id, c_name, c_desc FROM customer_tab WHERE c_id IN ('c_001', 'c_003')"
    procTimePrint(whereInSQL)
    println("groupbySQL------------------------华丽的分割线----------------------------")

    val groupbySQL = "SELECT c_id, count(o_id) as o_count FROM order_tab GROUP BY c_id"
    procTimePrint(groupbySQL)

    println("unionAllSQL------------------------华丽的分割线----------------------------")

    val unionAllSQL =
      """
        |SELECT c_id, c_name, c_desc  FROM customer_tab
        |UNION ALL
        |SELECT c_id, c_name, c_desc  FROM customer_tab
        |
      """.stripMargin
    procTimePrint(unionAllSQL)

    println("unionSQL------------------------华丽的分割线----------------------------")

    val unionSQL =
      """
        |SELECT c_id, c_name, c_desc  FROM customer_tab
        |UNION
        |SELECT c_id, c_name, c_desc  FROM customer_tab
        |
      """.stripMargin
    procTimePrint(unionSQL)

    println("joinSQL------------------------华丽的分割线----------------------------")

    val joinSQL = "SELECT * FROM customer_tab AS c JOIN order_tab AS o ON o.c_id = c.c_id"
    procTimePrint(joinSQL)

    println("leftjoinSQL------------------------华丽的分割线----------------------------")

    val leftjoinSQL = "SELECT * FROM customer_tab AS c LEFT JOIN order_tab AS o ON o.c_id = c.c_id"
    procTimePrint(leftjoinSQL)

    println("------------------------华丽的分割线----------------------------")
    println("------------------------华丽的分割线----------------------------")
    println("------------------------华丽的分割线----------------------------")

  }


  val _tempFolder = new TemporaryFolder

  @Rule
  def tempFolder: TemporaryFolder = _tempFolder

  def getStateBackend: StateBackend = {
    new MemoryStateBackend()
  }

  /**
    * 客户表数据
    * c_id	| c_name |  c_desc
    * c_001	| Kevin	 |     from  JinLin
    * c_002	| Sunny	 |  fromJinLin
    * c_003	| JinCheng| from HeBei
    *
    */
  val customer_data = new mutable.MutableList[(String, String, String)]
  customer_data.+=(("c_001", "Kevin", "from JinLin"))
  customer_data.+=(("c_002", "Sunny", "from JinLin"))
  customer_data.+=(("c_003", "JinCheng", "from HeBei"))


  /** 订单表数据
    * o_id	| c_id	 |  o_time	             | o_desc
    * o_oo1	| c_002 |  	2018-11-05 10:01:01 |   iphone
    * o_002	| c_001 |  	2018-11-05 10:01:55 |   ipad
    * o_003	| c_001 |  	2018-11-05 10:03:44 |  flink book
    */
  val order_data = new mutable.MutableList[(String, String, String, String)]
  order_data.+=(("o_001", "c_002", "2018-11-05 10:01:01", "iphone"))
  order_data.+=(("o_002", "c_001", "2018-11-05 10:01:55", "ipad"))
  order_data.+=(("o_003", "c_001", "2018-11-05 10:03:44", "flink book"))

  /** 商品销售表数据
    *
    * itemID	itemType	onSellTime	price
    * ITEM001	Electronic	2017-11-11 10:01:00	20
    * ITEM002	Electronic	2017-11-11 10:02:00	50
    * ITEM003	Electronic	2017-11-11 10:03:00	30
    * ITEM004	Electronic	2017-11-11 10:03:00	60
    * ITEM005	Electronic	2017-11-11 10:05:00	40
    * ITEM006	Electronic	2017-11-11 10:06:00	20
    * ITEM007	Electronic	2017-11-11 10:07:00	70
    * ITEM008	Clothes	2017-11-11 10:08:00	20
    */
  val item_data = Seq(
    Left((1510365660000L, (1510365660000L, 20, "ITEM001", "Electronic"))),
    Right((1510365660000L)),
    Left((1510365720000L, (1510365720000L, 50, "ITEM002", "Electronic"))),
    Right((1510365720000L)),
    Left((1510365780000L, (1510365780000L, 30, "ITEM003", "Electronic"))),
    Left((1510365780000L, (1510365780000L, 60, "ITEM004", "Electronic"))),
    Right((1510365780000L)),
    Left((1510365900000L, (1510365900000L, 40, "ITEM005", "Electronic"))),
    Right((1510365900000L)),
    Left((1510365960000L, (1510365960000L, 20, "ITEM006", "Electronic"))),
    Right((1510365960000L)),
    Left((1510366020000L, (1510366020000L, 70, "ITEM007", "Electronic"))),
    Right((1510366020000L)),
    Left((1510366080000L, (1510366080000L, 20, "ITEM008", "Clothes"))),
    Right((151036608000L)))

  /** 页面访问表数据
    * region	userId	accessTime
    * ShangHai	U0010	2017-11-11 10:01:00
    * BeiJing	U1001	2017-11-11 10:01:00
    * BeiJing	U2032	2017-11-11 10:10:00
    * BeiJing	U1100	2017-11-11 10:11:00
    * ShangHai	U0011	2017-11-11 12:10:00
    */
  val pageAccess_data = Seq(
    Left((1510365660000L, (1510365660000L, "ShangHai", "U0010"))),
    Right((1510365660000L)),
    Left((1510365660000L, (1510365660000L, "BeiJing", "U1001"))),
    Right((1510365660000L)),
    Left((1510366200000L, (1510366200000L, "BeiJing", "U2032"))),
    Right((1510366200000L)),
    Left((1510366260000L, (1510366260000L, "BeiJing", "U1100"))),
    Right((1510366260000L)),
    Left((1510373400000L, (1510373400000L, "ShangHai", "U0011"))),
    Right((1510373400000L)))

  /** 页面访问量表数据2
    * region	userCount	accessTime
    * ShangHai	100	2017.11.11 10:01:00
    * BeiJing	86	2017.11.11 10:01:00
    * BeiJing	210	2017.11.11 10:06:00
    * BeiJing	33	2017.11.11 10:10:00
    * ShangHai	129	2017.11.11 12:10:00
    */
  val pageAccessCount_data = Seq(
    Left((1510365660000L, (1510365660000L, "ShangHai", 100))),
    Right((1510365660000L)),
    Left((1510365660000L, (1510365660000L, "BeiJing", 86))),
    Right((1510365660000L)),
    Left((1510365960000L, (1510365960000L, "BeiJing", 210))),
    Right((1510366200000L)),
    Left((1510366200000L, (1510366200000L, "BeiJing", 33))),
    Right((1510366200000L)),
    Left((1510373400000L, (1510373400000L, "ShangHai", 129))),
    Right((1510373400000L)))

  // 页面访问表数据3
  val pageAccessSession_data = Seq(
    Left((1510365660000L, (1510365660000L, "ShangHai", "U0011"))),
    Right((1510365660000L)),
    Left((1510365720000L, (1510365720000L, "ShangHai", "U0012"))),
    Right((1510365720000L)),
    Left((1510365720000L, (1510365720000L, "ShangHai", "U0013"))),
    Right((1510365720000L)),
    Left((1510365900000L, (1510365900000L, "ShangHai", "U0015"))),
    Right((1510365900000L)),
    Left((1510366200000L, (1510366200000L, "ShangHai", "U0011"))),
    Right((1510366200000L)),
    Left((1510366200000L, (1510366200000L, "BeiJing", "U2010"))),
    Right((1510366200000L)),
    Left((1510366260000L, (1510366260000L, "ShangHai", "U0011"))),
    Right((1510366260000L)),
    Left((1510373760000L, (1510373760000L, "ShangHai", "U0410"))),
    Right((1510373760000L)))

  def procTimePrint(sql: String): Unit = {
    // Streaming 环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    // 将order_tab, customer_tab 注册到catalog
    val customer = env.fromCollection(customer_data).toTable(tEnv).as('c_id, 'c_name, 'c_desc)
    val order = env.fromCollection(order_data).toTable(tEnv).as('o_id, 'c_id, 'o_time, 'o_desc)

    tEnv.registerTable("order_tab", order)
    tEnv.registerTable("customer_tab", customer)

    val result = tEnv.sqlQuery(sql).toRetractStream[Row]
    val sink = new RetractingSink
    result.addSink(sink)
    env.execute()
  }

  def rowTimePrint(sql: String): Unit = {
    // Streaming 环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setStateBackend(getStateBackend)
    env.setParallelism(1)
    val tEnv = TableEnvironment.getTableEnvironment(env)

    // 将item_tab, pageAccess_tab 注册到catalog
    val item =
      env.addSource(new EventTimeSourceFunction[(Long, Int, String, String)](item_data))
        .toTable(tEnv, 'onSellTime, 'price, 'itemID, 'itemType, 'rowtime.rowtime)

    val pageAccess =
      env.addSource(new EventTimeSourceFunction[(Long, String, String)](pageAccess_data))
        .toTable(tEnv, 'accessTime, 'region, 'userId, 'rowtime.rowtime)

    val pageAccessCount =
      env.addSource(new EventTimeSourceFunction[(Long, String, Int)](pageAccessCount_data))
        .toTable(tEnv, 'accessTime, 'region, 'accessCount, 'rowtime.rowtime)

    val pageAccessSession =
      env.addSource(new EventTimeSourceFunction[(Long, String, String)](pageAccessSession_data))
        .toTable(tEnv, 'accessTime, 'region, 'userId, 'rowtime.rowtime)

    tEnv.registerTable("item_tab", item)
    tEnv.registerTable("pageAccess_tab", pageAccess)
    tEnv.registerTable("pageAccessCount_tab", pageAccessCount)
    tEnv.registerTable("pageAccessSession_tab", pageAccessSession)

    val result = tEnv.sqlQuery(sql).toRetractStream[Row]
    val sink = new RetractingSink
    result.addSink(sink)
    env.execute()
  }



}

// 自定义Sink
final class RetractingSink extends RichSinkFunction[(Boolean, Row)] {
  var retractedResults: ArrayBuffer[String] = mutable.ArrayBuffer.empty[String]

  def invoke(v: (Boolean, Row)) {
    retractedResults.synchronized {
      val value = v._2.toString
      if (v._1) {
        retractedResults += value
      } else {
        val idx = retractedResults.indexOf(value)
        if (idx >= 0) {
          retractedResults.remove(idx)
        } else {
          throw new RuntimeException("Tried to retract a value that wasn't added first. " +
            "This is probably an incorrectly implemented test. " +
            "Try to set the parallelism of the sink to 1.")
        }
      }
    }
    retractedResults.sorted.foreach(println(_))

  }
}

// Water mark 生成器
class EventTimeSourceFunction[T](
                                  dataWithTimestampList: Seq[Either[(Long, T), Long]]) extends SourceFunction[T] {
  override def run(ctx: SourceContext[T]): Unit = {
    dataWithTimestampList.foreach {
      case Left(t) => ctx.collectWithTimestamp(t._2, t._1)
      case Right(w) => ctx.emitWatermark(new Watermark(w))
    }
  }

  override def cancel(): Unit = ???
}
