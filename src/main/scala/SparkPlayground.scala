import org.apache.spark.sql.{SparkSession, _}

object SparkPlayground extends App {


  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Spark Playground")
      .master("local")
      .getOrCreate()


  val interface1 = spark.read.options(Map("header" -> "true", "inferSchema" -> "true"))
    .csv("/Users/alexeyyudin/Downloads/Evotor_interface_20200402.csv")

  val interface2 = spark.read.options(Map("header" -> "true", "inferSchema" -> "true"))
    .csv("/Users/alexeyyudin/Downloads/Evotor_interface_20200402-2.csv")

  val regionsCol: Column = interface1("GEO_REGION")


  import org.apache.spark.sql.functions._

  val oldRegions: DataFrame = interface1.select(col("GEO_REGION").as("old_region"), col("DEVICE_ID"))
  val newRegions: DataFrame = interface2.select(col("GEO_REGION").as("new_region"), col("DEVICE_ID"))
  val result = oldRegions.join(newRegions, Seq("DEVICE_ID"), "full")


  val finalResult = result.withColumn("region",
    when(
      col("old_region").isNull, col("new_region"))
      .otherwise(col("old_region"))
  )
    .drop("new_region", "old_region")


  println("null fina regions: " +
    finalResult.filter(row => {
      Option(row.getAs[String]("region")).isEmpty
    }).count()
  )

  println("null old regions: " +
    result.filter(row => {
      Option(row.getAs[String]("old_region")).isEmpty
    }).count()
  )


  spark.close()
}
