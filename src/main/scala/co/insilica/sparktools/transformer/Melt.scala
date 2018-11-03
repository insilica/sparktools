package co.insilica.sparktools.transformer

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Dataset, Row, functions}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import co.insilica.functional._


object Melt{

  object columns{
    val variable = "variable"
    val value = "value"
  }

}

case class Melt(meltColumns: String*) extends Transformer{

  override def transform(in: Dataset[_]): DataFrame = {
    import Melt.columns._
    val nonMeltColumns =  in.columns.filterNot{ meltColumns.contains }
    val newDS = in
      .select(nonMeltColumns.head,meltColumns:_*)
      .withColumn(variable, functions.lit(nonMeltColumns.head))
      .withColumnRenamed(nonMeltColumns.head,value)

    nonMeltColumns.tail
      .foldLeft(newDS){ case (acc,col) =>
        in
          .select(col,meltColumns:_*)
          .withColumn(variable, functions.lit(col))
          .withColumnRenamed(col,value)
          .union(acc)
      }
      .|>{
        case df if meltColumns.nonEmpty => df.select(meltColumns.head,meltColumns.tail ++ List(variable,value) : _*)
        case df => df.select(variable,value)
      }

  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = ???

  override val uid: String = Identifiable.randomUID("Melt")
}