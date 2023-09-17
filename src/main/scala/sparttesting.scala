import org.apache.spark.SparkContext;
//import org.apache.spark.sql.{SaveMode, SparkSession}
import java.util
object sparttesting {

  def main(args: Array[String]): Unit = {

    val sc=new SparkContext("local[4]","Spark_dev")
    val rdd1=sc.textFile("C:/Users/Vinothkumar/Desktop/testing.txt");
   //val rdd2 = rdd1.map(x=>x.split(" "))
   // rdd2.foreach(println)

    val  rdd2=rdd1.flatMap(x=>x.split(" "));
    val rdd3=rdd2.map(x=>(x,1))
    val rdd4=rdd3.reduceByKey((x,y)=>x+y)
    rdd4.collect.foreach(println)
   // scala.io.StdIn.readLine()

  }
}