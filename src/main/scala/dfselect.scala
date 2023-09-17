import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, monotonically_increasing_id, when}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object dfselect {
  def main(args: Array[String]): Unit = {
    // Create a SparkSession

    val spark = SparkSession.builder.appName("OOMExample").master("local[4]").getOrCreate()

    import spark.implicits._
    val df = List(("john", 25, "Hyd"),
                   ("mohan", 56, "mum"),
                    ("meera", 67, "AP"),
                    ("meher", 89, "Hyd")).toDF("name", "mark", "city")

    /*df.select(col("name"),col("city"),col("mark")
      ,when(col("age")>20 && col("age")<30,"ADULT")
        .otherwise("OLD").as("category")).show()*/

   df.select(col("name"), col("city"), col("mark"),
                      when(col("mark") < 30 , "fail").
                       when(col("mark") > 30 && col("mark") < 60, "pass")
                        .otherwise("first class").as("category")).show()

    scala.io.StdIn.readLine()
  }

}