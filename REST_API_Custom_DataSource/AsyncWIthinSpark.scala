package com.spark.demos
import org.apache.spark._
import scala.util.{Success, _}
import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

object ConcurrentContext {
  import scala.util._
  import scala.concurrent.duration.Duration
  import scala.concurrent.duration.Duration._

  def executeAsync[T](f: => T):Future[T] = Future(f)
  def futureToFutureOption[T](f: Future[T]): Future[Option[T]] =
    f.map(Some(_)).recover {
      case e => None
    }
  def awaitAll(iter:Iterator[Future[(Int,Try[Int])]], timeout:Duration = 3.second) = {
    Await.result(Future.sequence(iter), timeout)
  }
  def awaitBatch[T](iter:Iterator[Future[(Int,Try[Int])]], batchSize:Int =  10, timeout:Duration = 5.second) ={
    val grouped = iter.grouped(batchSize).map{x => Future.sequence(x)}
    iter.grouped(batchSize)
      .map(batch => Future.sequence(batch))
      .flatMap{futureBatch =>  Await.result(futureBatch, timeout).toIterator
      }
  }
}
object AsyncWithinSpark {
  
  def slowFunc(x:Int):(Int, Try[Int])={
    println(s"slowFunc start ($x)")
    val res =  Try(if(x.isInstanceOf[Int] && x%10 == 0) (x.asInstanceOf[Int]/0).asInstanceOf[Int] else x.asInstanceOf[Int])
    println(s"slowFunc end($x)")
    (x,res)
  }
  def fastFunc[T](x:T):T = {
    println(s"fastFunc($x)")
    x
  }

  def main(args:Array[String]): Unit ={

    val conf = new SparkConf().setMaster("spark://<ip>:7077").setAppName("ConcurrencySpark_Demo").set("spark.driver.host","localhost")
    val sc = new SparkContext(conf)
    val intRdd = sc.parallelize(1 to 20, 2)
    println("Async Results")
    val asyncRes = intRdd.map{ele => fastFunc(ele)}.map{ele =>ConcurrentContext.executeAsync(slowFunc(ele)) }
      .mapPartitions{
      it =>
       /* Unbounded concurrent execution using Futures  */
       // ConcurrentContext.awaitAll(it).map{case(ele:Int, tryEle:Try[Int]) =>

        // Batched concurrent execution, awaiting each batchSize which solves unbouded parallelism and OOM
        ConcurrentContext.awaitBatch(it).map{case(ele:Int, tryEle:Try[Int]) =>
        (ele.toString,
          tryEle match {
            case Success(ele) => ele.toString
            case Failure(ex) => "ERROR:" +ex.getMessage} )
      }
    }
    asyncRes.collect().foreach(println)
  }

}
