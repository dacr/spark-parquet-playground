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
  
  // http://blog.cloudera.com/blog/2016/04/benchmarking-apache-parquet-the-allstate-experience/
  
  def main(args:Array[String]):Unit = {
    parquetThroughSpark()
  }
  
  
  
  def parquetThroughJavaAPI():Unit = {
    
    // https://github.com/twitter/scalding/tree/develop/scalding-parquet
    
    import com.twitter.scalding.parquet._
    import com.twitter.scalding.parquet.tuple.macros.Macros._
    import com.twitter.scalding.parquet.tuple._
    import com.twitter.scalding.{ Args, Job, TypedTsv }
    import org.apache.parquet.filter2.predicate.{ FilterApi, FilterPredicate }
    import com.twitter.scalding.typed.TypedPipe
    import org.apache.parquet.io.api.Binary
    import org.apache.parquet.filter2.predicate.FilterApi.binaryColumn


    case class SampleClass(value: Long, square:Long, comment: String)

    
    
    class WriteToTypedParquetTupleJob(args: Args) extends Job(args) {
      val outputPath = args.required("output")
      val sink = TypedParquetSink[SampleClass](outputPath)
  
      TypedPipe.from(List(SampleClass(0, 1, "foo"), SampleClass(1, 2, "bar"))).write(sink)
    }
  
    
    
    class ReadWithFilterPredicateJob(args: Args) extends Job(args) {
      val fp: FilterPredicate = FilterApi.eq(binaryColumn("comment"), Binary.fromString("foo"))
  
      val inputPath = args.required("input")
      val outputPath = args.required("output")
  
      val input = TypedParquet[SampleClass](inputPath, fp)
  
      TypedPipe.from(input).map(_.square).write(TypedTsv[Int](outputPath))
    }
    
  }
  
  
  def parquetThroughSpark():Unit = {
    
    import org.apache.spark.SparkConf
    import org.apache.spark.sql.SparkSession
    import org.apache.spark.sql.SaveMode
    import scala.sys.process._

    val spark = SparkSession
      .builder()
      .master("local[1]")
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
    def howlong[T](proc : =>T):(Float, T) = {
      val started = System.currentTimeMillis
      val result = proc
      val ended = System.currentTimeMillis
      ( (ended-started).toFloat, result)
    }

    // ----------------------------------------------------------------------------------
    val filename="dummy.txt"
    howlong {
      val pw = new java.io.PrintWriter(filename)
      pw.println("value\tsquare\tcomment")
      for {
        i<- seq
        ii=i*i
      } pw.println(s"$i\t$ii\t$comment")
      pw.close()
    } match { case (dur,_) =>
      val sz = new java.io.File(filename).length()/1024/1024
      println(s"file write duration=${dur/1000}s size=${sz}Mb")
    }

    // ----------------------------------------------------------------------------------
    howlong {
        val count = scala.io.Source.fromFile(filename)
          .getLines
          .drop(1)
          .map(_.split("\t"))
          .map { case Array(value,square,comment) => (value.toLong, square.toLong, comment) }
          .filter { case (n, ns, c) => ns % 2 == 1}
          .size
        println(s"how many odd number=$count")
    } match { case (dur,_) =>
      println(s"file read duration : ${dur/1000}s")
    }
    
    // ----------------------------------------------------------------------------------
    howlong {
      val df2 = sparkContext.makeRDD(seq).map(i => (i, i * i, comment)).toDF("value", "square", "comment")
      val writer = df2.write.mode(SaveMode.Overwrite)
      writer.parquet("data/dummytable")
    } match { case (dur,_) =>
      val sz = ("du -d 0 data".!!).split("\\s+",2).head.toLong/1024/1024
      println(s"parquet write duration=${dur/1000}s size=${sz}Mb TODO write in asynchronous !")
    }
    
    
    // ----------------------------------------------------------------------------------
    howlong {
      val count = 
        read
          .parquet("data/dummytable")
          .filter("square % 2 == 1")
          .count
      println(s"how many odd number=$count")
      //rdf.show(10)
    } match { case (dur,_) =>
      println(s"parquet read duration : ${dur/1000}s")
    }


    sparkContext.stop()
  }
}
