import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{col, monotonically_increasing_id, when}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.SparkContext;
import scala.io.Source;

object api_date {
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val sc=new SparkContext("local[4]","Spark_dev")
    val spark = SparkSession.builder.appName("OOMExample").master("local[4]").getOrCreate()
    spark.sparkContext.setLogLevel("error")
    import spark.implicits._

    /* pulling the data from URL and and converting it into string */
    val urldata  = Source.fromURL("https://api.fda.gov/drug/event.json?search=receivedate:[20040101+TO+20081231]&limit=1")
    val strdata = urldata.mkString
    println(strdata);

    /* Converting the string JSON data into RDD , which help for reading in spark */
    val rdd = sc.parallelize(List(strdata))
    val df = spark.read.json(rdd)

    /* Flatterning the array column in first level hierarchy*/
    // df.printSchema()

    val narrowdf = df
      .withColumn("results", explode(col("results")))
      .withColumn("result_patient_drug", explode(col("results.patient.drug")))
      .withColumn("result_patient_reaction", explode(col("results.patient.reaction")))
      .withColumn("year", year(to_date(col("results.transmissiondate"), "yyyyMMdd")))
      .withColumn("month",month(to_date(col("results.transmissiondate"), "yyyyMMdd")))
      .withColumn("date", dayofmonth(to_date(col("results.transmissiondate"), "yyyyMMdd")))
    narrowdf.show(false)
    narrowdf.printSchema()

    /* selecting all the flatten column in main dataframe to form as table */

    val maindf = narrowdf.
      select(
      col("meta.disclaimer"),
      col("meta.last_updated"),
      col("meta.license"),
      col("meta.results.limit"),
      col("meta.results.skip"),
      col("meta.results.total"),
      col("meta.terms"),

      col("results.companynumb"),
      col("results.fulfillexpeditecriteria"),
      col("results.patient.patientdeath.patientdeathdate"),
      col("results.patient.patientdeath.patientdeathdateformat"),

      col ("results.patient.patientonsetage"),
      col("results.patient.patientonsetageunit"),
      col("results.patient.patientsex"),

      col ("results.primarysource.qualification"),
      col("results.primarysource.reportercountry"),

      col("results.receiptdate"),
      col("results.receiptdateformat"),
      col("results.receivedate"),
      col("results.receivedateformat"),
      col("results.receiver"),
      col("results.safetyreportid"),
      col("results.sender.senderorganization"),
      col("results.serious"),
      col("results.seriousnessdeath"),

      col("results.transmissiondate"),
      col("results.transmissiondateformat"),

      col("result_patient_drug.drugadministrationroute"),
      col("result_patient_drug.drugauthorizationnumb"),
      col("result_patient_drug.drugcharacterization"),
      col("result_patient_drug.drugindication"),
      col("result_patient_drug.medicinalproduct"),
      col("result_patient_reaction.reactionmeddrapt"),
      col("year"),
      col("month"),
      col("date")
    )

    /* Adding seperate year,month,date column based on Transmission date to partition the data based on it */
    /* Cache/persist the dataframe before performing the action in it*/

    /*maindf
    .write
    .format("csv")
    .option("header","true")
    .partitionBy("year","month","date")
    .mode("append")
      .save("file:///C:/Users/Vinothkumar/Desktop/vinoth/maindf")*/

     maindf.write
      .format("parquet")
      .option("header", "true")
      .partitionBy("year", "month", "date")
      .mode("append")
      .saveAsTable("database_name.table_name")

    maindf.show()
   //maindf.printSchema()
  }
}
