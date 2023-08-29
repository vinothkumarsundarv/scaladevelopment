import scala.io.StdIn
object assignment {

  def main (args : Array[String])={
    println("enter the target value")
    var cnt = StdIn.readInt();
    while (cnt <=20){
   if (cnt%2==0){
     println("result "+cnt);
     cnt =cnt+2;
    }
      cnt = cnt+ 1;
    }





  }
}
