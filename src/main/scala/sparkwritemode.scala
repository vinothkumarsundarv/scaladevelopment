import org.apache.spark.sql.{SaveMode, SparkSession}
  import org.apache.spark.sql.functions.{col, monotonically_increasing_id}
  import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

  object sparkwritemode {
    def main(args: Array[String]): Unit = {
      // Create a SparkSession

      val spark = SparkSession.builder.appName("OOMExample").master("local[4]").getOrCreate()

      val df=spark.read
        .format("csv")
        .option("header",true)
        .option("inferSchema",true)
        .option("path","C:/Users/Vinothkumar/Desktop/seekho/data.csv")
        .load()

      df.write
        .format("csv")
        .mode(SaveMode.Ignore)
        .option("path","C:/Users/Vinothkumar/Desktop/seekho/output")
        .save()

     // scala.io.StdIn.readLine()
    }

  }
