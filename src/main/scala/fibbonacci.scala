import scala.io.StdIn
object fibbonacci {

  def main (args : Array[String])={
    val limit = 40;
    var num1 = 0;
    var num2 = 1;

    var sum : Int = num1+num2
    var i = 0;

      while (sum <= limit) {
        println(sum)
        num1 = num2;
        num2 = sum;
        sum = num1 + num2;
    }
  }
}
