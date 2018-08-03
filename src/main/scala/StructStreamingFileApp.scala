
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory


object StructStreamingFileApp extends App {

  private val log = LoggerFactory.getLogger(this.getClass)

  val spark = SparkSession.builder()
    .master("local[*]")
    .config("spark.driver.memory", "2g")
    .appName("spark_local")
    .enableHiveSupport()
    .getOrCreate()
  spark
    .sparkContext
    .setLogLevel("WARN")

  import spark.implicits._
  import org.apache.spark.sql.functions._


  val schemaExp = StructType(
    StructField("unix_time", TimestampType, nullable = false) ::
    StructField("category_id", IntegerType, nullable = false) ::
    StructField("ip", StringType, nullable = false) ::
    StructField("type", StringType, nullable = false) :: Nil
  )


  val in = spark.readStream
    .schema(schemaExp)
    .format("json")
    .option("path", "/Users/mkonstantinov/IdeaProjects/streaming_task/events")
    .load()

  val out = in
      .withWatermark("unix_time", "5 minutes")
    .groupBy(
      window(col("unix_time"), "5 minutes")
//      ,
//      col("unix_time")
    )
    .count()
    .writeStream
    .format("console")
    .outputMode(OutputMode.Update())
    .start()

  out.awaitTermination()


}


