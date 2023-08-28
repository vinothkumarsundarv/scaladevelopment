object testing {
  def kgtog(a: Int) = {
    val x = a * 1000;
    println("Total gram " + a + " is " + x)
  }

  def CtoF(a: Int) = {
    val x = (a * 9 / 5) + 32
    println("The result of " + a + " degree in F is " + x + "")
  }

  def biggestnumber(a: Int, b: Int, c: Int) = {
    if (a > b && a > c) {
      println("Biggest number is " + a)
    }
    else if (b > a && b > c) {
      println("Biggest number is " + b)
    }
    else {
      println("Bigest number is " + c)
    }

  }

  def range(num: Int) = {
    if (num > 100 && num < 1000) {
      if (num % 2 == 0) {
        println("Given number is Even")
        val res = num % 3
        println("The result of divide by 3 is " + res)
      }
      else {
        println("Given number is odd")
        val res = num % 2
        println("The result of divide by 2 is " + res)
      }
    }
    else {
      println("wrong Number")
    }

  }

  def range100(num: Int) = {
    if (num >= 0 && num <= 100) {
      if (num > 90 && num <= 100) {
        println("Super Smart")
      }
      else if (num > 80 && num <= 90) {
        println("Smart")
      }
      else if (num > 70 && num <= 80) {
        println("Smart enough")
      }
      else if (num > 60 && num <= 70) {
        println("just smart")
      }
      else if (num > 35 && num <= 60) {
        println("no smart")
      }
      else if (num > 0 && num <= 35) {
        println("dump")
      }
    }
    else {
      println("invalid input")
    }
  }

  def mathfunc(a: Int, b: Int, operator: Char) = {
    if (operator == '+') {
      println("addition", a + b);
    }
    else if (operator == '-') {
      println("Subtract", a - b);
    }
    else if (operator == '+') {
      println("Multiplication", a * b);
    }
    else if (operator == '+') {
      println("Division", a / b);
    }
  }

  def calcSum(x: Int, y: Int): Unit = {
    var a: Int = 0;
    var sum: Int = 0;
    for (a <- x to y) {
      sum = sum + a
      println("inner" + sum)
    }
    println(sum)
  }

  def evennumber(a: Int, b: Int) = {

    for (i <- a to b) {
      if (i % 2 == 0) {
        println("even number " + i)
      }
    }

  }

  def main(args: Array[String]): Unit = {
    kgtog(10);
    CtoF(80);
    biggestnumber(40, 20, 30);
    range(101);
    range100(101);
    mathfunc(5, 7, '-');
    calcSum(5, 10);
    evennumber(10, 20);

    var cnt = 1;
    while (cnt <= 5) {
      println("seeko bigdata ");
      cnt = cnt + 1;
    }
    for (i <- 10 to 77) {
      if (i % 11 == 0) {
        println(i);
      }
    }


  }
}
