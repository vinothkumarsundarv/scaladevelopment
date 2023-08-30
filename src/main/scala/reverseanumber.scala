import scala.io.StdIn
object reverseanumber {
  def main (args : Array[String])={
    println("enter the 3 digit number to reverse ")
    var num = StdIn.readInt()
    val bckp_num = num;
    var sum=0;
    while (num != 0)
      {
        val rem = num%10;
        sum = sum*10+rem;
        num = num/10;
      }
     if (bckp_num==sum)
     {
       println("this is a palindrome "+ sum)
     }
     else
       {
         println("this is not a palindraome "+ sum)
       }
  }
}
