package co.insilica.sparktools.transformer

import breeze.linalg.{DenseMatrix, DenseVector, Matrix, cholesky, inv}
import breeze.numerics.lgamma
import breeze.stats.distributions.Gaussian
import co.insilica.functional._
import co.insilica.sparktools.transformer.Melt
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.Transformer
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.mllib.linalg.distributed.{BlockMatrix, CoordinateMatrix, MatrixEntry, RowMatrix}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction


/**
  * Converts a dataset of form
  *
  * indexColumn | variableColumn | pValue
  * ----------- | -------------- | ------
  *
  * into independent p-values using the truncated product method
  * https://pdfs.semanticscholar.org/3684/f6cb105faa6723c9c309cff85bd4589ac207.pdf
  *
  * return is
  * indexColumn | variableColumn | pValue | tpmPValue |
  * ----------- | -------------- | ------ | --------- |
  * //TODO currently requires indexCol is a string
  *
  * @param indexCol index column. eg. patient identifiers
  * @param varCol variable column. eg. gene nanes
  * @param pCol p value column. eg. confidence that variable affects patient
  */
case class TruncatedProductMethod(indexCol:String, varCol:String, pCol:String,
                                  tau:Double = 0.05, choleskyCorrection:Boolean=true)
  extends Transformer{

  override def transform(ds: Dataset[_]): DataFrame = {
    import ds.sparkSession.implicits._
    import TruncatedProductMethod.columns
    // keep an index for each variable and index of indexes
    val variables = ds.select(varCol).distinct().collect().map{ _.getString(0) }.toList

    //find correlations between variable pvalues
    val correlations = this.correlationMatrix(ds,variables)
    println(correlations)
    val invCholesky: DenseMatrix[Double] = inv(cholesky(correlations))

    //find zscores for each pValue pivot to columns for variables
    ds
      .groupBy(indexCol)
      .pivot(varCol)
      .agg(functions.first(pCol))
      .withColumn("pArray",functions.array(variables.head,variables.tail:_*))
      .withColumn("tpmArray", TPMUDF(invCholesky)($"pArray"))
      .|>{ df =>
        if(choleskyCorrection)
          df.withColumn(columns.tpm, TruncatedProductMethod.cdfUDF(tau){ $"tpmArray" })
        else
          df.withColumn(columns.tpm, TruncatedProductMethod.cdfUDF(tau){ $"pArray" })
      }

      .select(indexCol,columns.tpm)
  }

  def TPMUDF(invCholesky:DenseMatrix[Double]): UserDefinedFunction = functions.udf{ pvalues : Seq[Double] =>
    pvalues
      .map{ p => Gaussian(0,1).inverseCdf(1.0 - p) }
      .|>{ pValues => DenseVector(pValues:_*) }
      .|>{ zVec => invCholesky * zVec }
      .|>{ _.map( x => 1.0 - Gaussian(0,1).cdf(x) ).toArray }
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = ???

  def correlationMatrix(ds:Dataset[_], variables:List[String]) : DenseMatrix[Double] = {

    val correlations = variables
      .flatMap{ k1 => variables.map{ k2 => functions.corr(k1,k2) } }

    val correlationVector = ds
      .groupBy(indexCol).pivot(varCol).agg(functions.first(pCol))
      .agg(correlations.head,correlations.tail:_*)
      .first()
      .|>{ row => row.toSeq.map{_.asInstanceOf[Double]}.toArray }
      .|>{ arr => arr.map{ x => Math.round(x*Math.pow(10,9))/Math.pow(10,9)}}

    Matrix.create(rows =variables.size, cols = variables.size,correlationVector).toDenseMatrix
  }

  override val uid: String = Identifiable.randomUID("TruncatedProductMethod")
}

object TruncatedProductMethod{

  object columns{
     val tpm = "truncated_product_method"
  }

  /**
    * Equation (1) in https://pdfs.semanticscholar.org/3684/f6cb105faa6723c9c309cff85bd4589ac207.pdf
    * This works if you do not need to do a Correlation correction.
    * @param pvalues = product of p values less than tau
    * @param tau = maximum allowed p-value
    * @return cumulative distribution value
    */
  def cdf(pvalues:Seq[Double],tau:Double):Double = {
    val L = pvalues.size
    val w = pvalues.filter{_ <= tau}.foldLeft(1.0){ case (acc,p) => acc*p}
    if(w >= 1.0) 1.0
    else{
      def factorial(x:Int) = Math.exp(lgamma(x+1))
      def ln(x:Double) = Math.log(x)
      def kStep(k:Int) : Double = {
        val combinations = (1 to L).combinations(k).size
        def sStep(s:Int) : Double = Math.pow(k*ln(tau) - ln(w),s)/factorial(s)
        val sSum = w*(0 until k).foldLeft(0.0){ case (acc,s) => acc + sStep(s)}
        val kCoef = combinations * Math.pow(1.0 - tau,L-k)
        val kSum = Math.pow(tau,k) match{
          case tk if w <= tk => kCoef*sSum
          case tk => kCoef*tk
        }
        kSum
      }
      (1 to L).foldLeft(0.0){ case (acc, k) => acc + kStep(k)}
    }
  }

  def cdfUDF(tau:Double): UserDefinedFunction = functions.udf{pvalues : Seq[Double] =>
    TruncatedProductMethod.cdf(pvalues,tau)
  }
}
