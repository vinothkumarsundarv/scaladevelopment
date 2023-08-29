import scala.io.StdIn
object array_sum {

  def main(args: Array[String]) = {
    println("enter the size of array")
    var size = StdIn.readInt();
    val arr = new Array[Int] (size)
    for (i <- 0 until arr.length){
      println("enter the element  of array")
      arr(i)=StdIn.readInt()
    }
    var sum = 0;
    for (i<-0 until arr.size)
      {
        sum = sum + arr(i);
      }
    println(sum);
  }
}
