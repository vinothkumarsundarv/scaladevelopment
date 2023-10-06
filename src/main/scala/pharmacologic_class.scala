import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{col, monotonically_increasing_id, when}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.SparkContext;
import scala.io.Source;

object pharmacologic_class {

  def main(args: Array[String]): Unit = {
    // Create a SparkSession and spark context
    val sc = new SparkContext("local[4]", "Spark_dev")
    val spark = SparkSession.builder.appName("OOMExample").master("local[4]").getOrCreate()
    spark.sparkContext.setLogLevel("error")
    import spark.implicits._
    /* pulling the data from URL and and converting it into string */
    val urldata = Source.fromURL("https://api.fda.gov/drug/event.json?search=patient.drug.openfda.pharm_class_epc:\"nonsteroidal+anti-inflammatory+drug\"&limit=1")
    val strdata = urldata.mkString
    //println(strdata);
    /* Converting the string JSON data into RDD , which help for reading in spark */
    val rdd = sc.parallelize(List(strdata))
    val df = spark.read.json(rdd)

   //df.printSchema()

    /* Flatterning the array column in first level hierarchy*/
    // df.printSchema()

    val narrowdf = df
      .withColumn("results", explode(col("results")))
      .withColumn("result_patient_drug", explode(col("results.patient.drug")))

    /* Flatterning the array column in second  level hierarchy*/

      val narrowdf2 = narrowdf
      .withColumn("result_patient_drug_element_openfda_application_number", explode(col("result_patient_drug.openfda.application_number")))
      .withColumn("result_patient_drug_element_openfda_brand_name",         explode(col("result_patient_drug.openfda.brand_name")))
      .withColumn("result_patient_drug_element_openfda_generic_name", explode(col("result_patient_drug.openfda.generic_name")))
      .withColumn("result_patient_drug_element_openfda_manufacturer_name", explode(col("result_patient_drug.openfda.manufacturer_name")))
      .withColumn("result_patient_drug_element_openfda_nui", explode(col("result_patient_drug.openfda.nui")))
      .withColumn("result_patient_drug_element_openfda_package_ndc", explode(col("result_patient_drug.openfda.package_ndc")))
      .withColumn("result_patient_drug_element_openfda_pharm_class_cs", explode(col("result_patient_drug.openfda.pharm_class_cs")))
      .withColumn("result_patient_drug_element_openfda_pharm_class_epc", explode(col("result_patient_drug.openfda.pharm_class_epc")))
      .withColumn("result_patient_drug_element_openfda_pharm_class_moa", explode(col("result_patient_drug.openfda.pharm_class_moa")))
      .withColumn("result_patient_drug_element_openfda_product_ndc", explode(col("result_patient_drug.openfda.product_ndc")))
      .withColumn("result_patient_drug_element_openfda_product_type", explode(col("result_patient_drug.openfda.product_type")))
      .withColumn("result_patient_drug_element_openfda_route", explode(col("result_patient_drug.openfda.route")))
      .withColumn("result_patient_drug_element_openfda_rxcui", explode(col("result_patient_drug.openfda.rxcui")))
      .withColumn("result_patient_drug_element_openfda_spl_id", explode(col("result_patient_drug.openfda.spl_id")))
      .withColumn("result_patient_drug_element_openfda_spl_set_id", explode(col("result_patient_drug.openfda.spl_set_id")))
      .withColumn("result_patient_drug_element_openfda_substance_name", explode(col("result_patient_drug.openfda.substance_name")))
      .withColumn("result_patient_drug_element_openfda_unii", explode(col("result_patient_drug.openfda.unii")))
      .withColumn("result_patient_reaction", explode(col("results.patient.reaction")))
      .withColumn("year", year(to_date(col("results.transmissiondate"), "yyyyMMdd")))
      .withColumn("month", month(to_date(col("results.transmissiondate"), "yyyyMMdd")))
      .withColumn("date", dayofmonth(to_date(col("results.transmissiondate"), "yyyyMMdd")))

    //narrowdf2.printSchema()

    /* selecting all the flatten column in main dataframe to form as table   */

    val maindf = narrowdf2.
      select(
        col("results.companynumb").alias("result_companynumb"),
        col("results.duplicate").alias("result_duplicate"),
        col("results.fulfillexpeditecriteria").alias("result_fulfillexpeditecriteria"),

        col("result_patient_drug.drugadditional"),
        col("result_patient_drug.drugauthorizationnumb"),
        col("result_patient_drug.drugcharacterization"),
        col("result_patient_drug.drugindication"),
        col("result_patient_drug.drugrecurreadministration"),
        col("result_patient_drug.medicinalproduct"),

        col("result_patient_drug_element_openfda_application_number"),
        col("result_patient_drug_element_openfda_brand_name"),
        col("result_patient_drug_element_openfda_generic_name"),
        col("result_patient_drug_element_openfda_manufacturer_name"),
        col("result_patient_drug_element_openfda_nui"),
        col("result_patient_drug_element_openfda_package_ndc"),
        col("result_patient_drug_element_openfda_pharm_class_cs"),
        col("result_patient_drug_element_openfda_pharm_class_epc"),
        col("result_patient_drug_element_openfda_pharm_class_moa"),

        col("result_patient_drug_element_openfda_product_ndc"),
        col("result_patient_drug_element_openfda_product_type"),
        col("result_patient_drug_element_openfda_route"),
        col("result_patient_drug_element_openfda_rxcui"),
        col("result_patient_drug_element_openfda_spl_id"),
        col("result_patient_drug_element_openfda_spl_set_id"),
        col("result_patient_drug_element_openfda_substance_name"),
        col("result_patient_drug_element_openfda_unii"),

        col("results.patient.patientsex"),

        col("result_patient_reaction.reactionmeddrapt"),
        col("result_patient_reaction.reactionmeddraversionpt"),

        col("results.primarysource.qualification"),
        col("results.primarysource.reportercountry"),

        col("results.primarysourcecountry"),
        col("results.receiptdate"),
        col("results.receiptdateformat"),
        col("results.receivedate"),
        col("results.receivedateformat"),
        col("results.receiver.receiverorganization"),
        col("results.receiver.receivertype"),

        col("results.reportduplicate.duplicatenumb"),
        col("results.reportduplicate.duplicatesource"),

        col("results.reporttype"),
        col("results.safetyreportid"),
        col("results.safetyreportversion"),

        col("results.sender.senderorganization"),
        col("results.sender.sendertype"),

        col("results.serious"),
        col("results.seriousnessother"),
        col("results.transmissiondate"),
        col("results.transmissiondateformat"),
        col("year"),
        col("month"),
        col("date")
      )
    /* Adding seperate year,month,date column based on Transmission date to partition the data based on it */
    /* Cache/persist the dataframe before performing the action in it*/

    //maindf.cache()

    /* storing the dataframe a table in hive with partitioned, to avoid the full table scan on data*/
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
