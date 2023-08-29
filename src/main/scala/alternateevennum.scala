import scala.io.StdIn
object alternateevennum {

  def main(args:Array[String])={
    println("enter the target of even number")
    var tgt = StdIn.readInt();
    var cnt = 0;
    while (cnt<=tgt){
      if (cnt%2==0)
      {
        println(cnt);
        cnt =cnt+2;
      }
      cnt = cnt+1;
    }
  }
}
