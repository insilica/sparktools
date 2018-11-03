package co.insilica.sparktools

import co.insilica.sparktools.transformer._
import org.scalatest.WordSpec
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

class SparktoolsTest extends WordSpec {

  val spark = SparkSession.builder()
      .master("local[*]")
      .appName("sparktool-test")
      .getOrCreate()

  import spark.implicits._

  Logger.getRootLogger.setLevel(Level.ERROR)

  "Transformers" should {

    "melt a dataset" in {

      val MeltColumns = Range(3,10).map( i => StructField("name_"+i,DoubleType))

      val schema = StructType(
        List(StructField("Melt1",StringType),StructField("Melt2",StringType)) ++ MeltColumns)

      val rows = Range(1,11).map{ i => Row("a" :: "b" :: Range(3,10).map{ j => Math.random() }.toList :_ *)}
      val rowRDD = spark.sparkContext.parallelize(rows)
      val df = spark.createDataFrame(rowRDD,schema)
      val newDF = df.transform(Melt("Melt1","Melt2").transform)

      newDF.select("variable").distinct().map(_.toString()).collect().toSet ==
        )
      assert(newDF.count() === 70)
    }

    //example taken from https://www.r-bloggers.com/two-sample-students-t-test-1/
  //   "spark.statistics" should "generate two tailed t-test" in {

  //     //heights of girls vs boys
  //     val Girls = List(175, 168, 168, 190, 156, 181, 182, 175, 174, 179)
  //     val Boys = List(185, 169, 173, 173, 188, 186, 175, 174, 179, 180)

  //     val zipped = Girls.indices.map{ i => (Girls(i),Boys(i)) }
  //     val df = spark.sparkContext.parallelize(zipped)
  //       .toDF("M1","M0")
  //       .<|{ _.show()}
  //       .transform(Melt().transform)
  //       .withColumnRenamed("variable", "gender")
  //       .withColumn("metric", functions.lit("cm"))
  //       .transform{ df =>
  //         TTest_TwoSample(twoTailed=true,
  //           target="gender", variableColumn="metric", valueColumn = "value", targetOrder = -1, minTestNumber = 5
  //         ).transform(df)
  //       }
  //     df.show()
  //   }
  }
  
}


// package co.insilica.sparktests


// import breeze.stats.distributions.ChiSquared
// import co.insilica.borg.spark.SparkEnvironment
// import co.insilica.borg.spark.statistics.Statistics
// import co.insilica.borg.spark.statistics.transformers.{PearsonConfidence, TTest_TwoSample}
// import co.insilica.borg.spark.udaf.SparseVectorAgg
// import co.insilica.functional._
// import co.insilica.spark.transformer.Melt
// import com.quantifind.charts.Highcharts._
// import org.apache.spark.sql._
// import org.apache.spark.sql.functions._
// import org.apache.spark.sql.types._
// import org.apache.spark.streaming.{Seconds, StreamingContext}
// import org.scalatest.FlatSpec

// class Sparktests extends FlatSpec{

//   implicit val se: SparkSession = SparkEnvironment.localBorg.sparkSession
//   import se.implicits._

//   "spark" should "aggregate into sparse vector" in {

//     val ds = List(
//       ("id_a","a.1",1.0),
//       ("id_b","a.1",2.0),
//       ("id_c","a.1",3.0),
//       ("id_a","b.1",4.0),
//       ("id_b","b.1",5.0),
//       ("id_c","b.1",6.0)
//     ).toDF("nonPlyr","featureName","featureMetric")

//     val featureNameMap : Map[String,Long] = ds.select("featureName")
//       .distinct()
//       .collect()
//       .map{ row => row.getAs[String](0)}
//       .zipWithIndex
//       .map{ x => (x._1,x._2.toLong)}
//       .toMap

//     val sva = new SparseVectorAgg(featureNameMap)

//     ds
//       .groupBy("nonPlyr").agg(sva(ds("featureName"),ds("featureMetric")))
//       .show()
//   }

//  "spark" should "standardscaler a feature column" in {
//    val ds = Range(0,10000).map{ i =>
//      if(i < 5000) ("group",Math.random())
//      else ("group2",Math.random()+100)
//    }.toDF("group","values")
//
//    val values = ds.transform(StandardScaler("group","values").transform)
//      .<|{ _.show()}
//      .select("values_standard")
//      .collect()
//      .map{_.getAs[Double](0)}
//    histogram(values.toSeq)
//    Thread.sleep(10000)
//  }


//  "spark.statistics" should "generate pearson confidence" in {
//
//    (10 to 1000 by 10).foreach{ sampleSize =>
//      val source = (1 to sampleSize).map{ x => Math.random()}
//      val rows = source.map{ x =>
//        val corr = if(x > 0.5) 2*x else Math.random()
//        val uncorr = Math.random()
//        val invCorr = if(x > 0.5) -2*x else Math.random()
//        (x,corr,uncorr,invCorr)
//      }
//
//      val df = spark.sparkContext.parallelize(rows).toDF("source","correlated","uncorrelated","inverse correlated")
//
//      df
//        .transform(df => new Melt("source").transform(df))
//        .transform{ df =>
//          PearsonConfidence(variableColumn = M.variable, valueColumn = M.value, targetColumn = "source")
//            .transform(df)
//        }
//        .show(truncate = false)
//    }
//  }
//
//  def correlateds = List("a", "b","c")
//    .flatMap{ name =>
//      val randVals = (1 to 100).map{ i => (i,Math.random()/10) }.toMap
//      (1 to 100).map{
//        case x if name == "a" => (x,name,randVals(x))
//        case x if name == "b" => (x,name,randVals(x) + Math.random()/100)
//        case x if name == "c" => (x,name,randVals(x) + Math.random()/100)
//      }
//    }
//    .|>{ tuples => spark.sparkContext.parallelize(tuples)}
//    .toDF("index","name","value")
//
//  "spark.statistics" should "generate rowmatrix from long form" in {
//    Statistics
//      .longFormMatrix(correlateds,indexCol="index",varCol="name",valCol="value")
////      .<|{ mat => mat.toLocalMatrix().rowIter.foreach(println) }
//      .<|{ mat => mat.toRowMatrix().computeCovariance().rowIter.foreach(println) }
//  }
//
//  "spark" should "take log of 0?" in {
//    val df: DataFrame = List(0.0,1,2,3,4).toDF("numbers")
//    val minValue = df.where($"numbers">0).agg(functions.min("numbers")).first().getDouble(0)
//    val logUDF = udf{ t:Double => if(t <= 0) Math.log(minValue) else Math.log(t) }
//    df.withColumn("log",logUDF($"numbers")).show()
//  }
//
//  "breeze chisquared" should "generate pvalues" in {
//    (1 to 100 by 2).foreach{ x =>
//      println(ChiSquared(3).cdf(x/10.0))
//    }
//  }
// }