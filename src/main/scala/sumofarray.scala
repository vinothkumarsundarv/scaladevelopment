object sumofarray {
def main (args : Array[String])={
  val arr = Array(5,2,3,1,4);
  var i = 0;
  var a= 0;
  println("outer"+arr.length);
 // for (i <- arr){
   // a=a+i;
 //   println(a);
  //}
 // val matrix: Array[Array[Int]] = Array(Array(1, 2, 3), Array(4, 5, 6), Array(7, 8, 9))
  // println(matrix(1)(1));
   val squarenum:Array[Int]=arr.map(x=>x*x);
  squarenum.foreach(println);
  val filterarray:Array[Int]=arr.filter(_%2==0);
  filterarray.foreach(println);

}
}
