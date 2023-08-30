import scala.io.StdIn
object sumofdigit {

  def main(args : Array[String])={
    println("Enter the value to sum in 3 digit ")
    var num = StdIn.readInt();
    var sum = 0;
    while(num !=0)
      {
       var rem = num%10;  /*3*/
          sum = sum+rem; /*3*/
          num = num/10; /*12*/

      }
      println("the total of this 4 digit is " + sum);
  }

}
