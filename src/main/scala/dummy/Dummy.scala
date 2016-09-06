/*
 * Copyright 2016 David Crosson
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dummy

object Dummy {
  val userName = util.Properties.userName
  val message = s"Hello ${userName}."
  
  def main(args:Array[String]):Unit = {
    
    import org.apache.spark.SparkConf
    import org.apache.spark.sql.SparkSession
    import org.apache.spark.sql.SaveMode

    val spark = SparkSession
      .builder()
      .master("local[4]")
      .appName("Dummy")
      .config("spark.sql.parquet.compression.codec","lzo") // lzo, gzip, snappy, uncompressed
      .getOrCreate()
        
    import spark._
    import spark.implicits._
  
    sparkContext.setLogLevel("OFF")

    // Create a simple DataFrame, store into a partition directory
    //val df = sparkContext.makeRDD(1 to 5).map(i => (i, i * i)).toDF("value", "square")
    //df.write.mode(SaveMode.Overwrite).parquet("data/dummytable")

    
    def stream(i: Long = 1): Stream[Long] = i #:: stream(i + 1)
    def seq = stream(5000000L).take(20*1000*1000)
    def comment = List("trucABCDEF", "mucheABCDE")(util.Random.nextInt(2))
    def howlong[T](proc : =>T):(Long, T) = {
      val started = System.currentTimeMillis
      val result = proc
      val ended = System.currentTimeMillis
      (ended-started, result)
    }
    
    // ----------------------------------------------------------------------------------
    val (d1,_) = howlong {
      val df2 = sparkContext.makeRDD(seq).map(i => (i, i * i, comment)).toDF("value", "square", "comment")
      df2.write.mode(SaveMode.Overwrite).parquet("data/dummytable")
    }
    println(s"parquet write duration : ${d1/1000}s")
    // => 194Mb on disk - 26s
    
    // ----------------------------------------------------------------------------------
    val (d2,_) = howlong {
      val pw = new java.io.PrintWriter("dummy.txt")
      pw.println("value\tsquare\tcomment")
      for {
        i<- seq
        ii=i*i
      } pw.println(s"$i\t$ii\t$comment")
      pw.close()
    }
    println(s"file write duration : ${d2/1000}s")
    // => 677Mb on disk - 19s

    
    // ----------------------------------------------------------------------------------
    val (d3,_) = howlong {
      val rdf = read.parquet("data/dummytable")
      rdf.printSchema()
      println("count="+rdf.count())
      //rdf.show(10)
    }
    println(s"parquet read duration : ${d3/1000}s")

    // ----------------------------------------------------------------------------------
    val (d4,_) = howlong {
        val count = scala.io.Source.fromFile("dummy.txt")
          .getLines
          .drop(1)
          .map(_.split("\t"))
          .map { case Array(value,square,comment) => (value.toLong, square.toLong, comment) }
          .size
        println(s"count=$count")
    }
    println(s"file read duration : ${d4/1000}s")

    sparkContext.stop()
  }
}
