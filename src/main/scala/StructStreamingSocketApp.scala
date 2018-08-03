
import StructStreamingFileApp.schemaExp
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory


object StructStreamingSocketApp extends App {

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

  val lines = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load()


  val query = lines.writeStream
    .outputMode(OutputMode.Append)
    .format("console")
    .start()
  query.awaitTermination()

}


