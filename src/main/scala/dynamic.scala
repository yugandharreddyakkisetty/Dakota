import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

case class Transformation(
                         outputColumn :String,
                         dependentColumn:String,
                         command:String,
                         logic:String
                         )
object Interview1 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName("Interview").getOrCreate()
    spark.sparkContext.setLogLevel("Error")

    val transformations=List(
      Transformation("DirectMapColumn","AccountingDate","DirectMap","AccountingDate"),
      Transformation("StaticValueColumn","null","StaticValue","Yugandhar"),
      Transformation("AddPrefixColumn","AccountingDate","AddPrefix","AccountingDate|myPrefix"),
      Transformation("NullColumn","null","NullColumn","null"),
      Transformation("SubstringColumn","AccountingDate","Substring","AccountingDate|0|4"),
      Transformation("ChangeDateColumn","AccountingDate","ChangeDateFormat","AccountingDate|yyyy-MM-dd|dd-MM-yyyy"),
      Transformation("AddPrefixIfNotNull","ERPCommodityId","AddPrefixIfNotNull","PREFIX123-"),
      Transformation("Amount","Amount","FillNull","0.00|Amount")
    )
    val transformationsMap=transformations.map(x=>x.outputColumn -> x).toMap

    val outputColumns=transformationsMap.map(x=>x._1)
    

    val df = spark.read
      .option("header", true)
      .option("sep", ",")
      .option("charset", "UTF-8")
      .csv("sample.csv")



   val df3= outputColumns.foldLeft(df){
      case (tempdf,outputColumn) => {
        val T=transformationsMap(outputColumn)
        T.command match {
          case "DirectMap"   => tempdf.withColumn(outputColumn,col(T.logic))
          case "StaticValue" => tempdf.withColumn(outputColumn,lit(T.logic))
          case "AddPrefix"   => tempdf.withColumn(outputColumn,concat(lit(T.logic.split("\\|")(1)),col(T.logic.split("\\|")(0))))
          case "NullColumn"  => tempdf.withColumn(outputColumn,lit(null))
          case "Substring"   => tempdf.withColumn(outputColumn,substring(col(T.logic.split("\\|")(0)),T.logic.split("\\|")(1).toInt,T.logic.split("\\|")(2).toInt))
          case "AddSuffix"   => tempdf.withColumn(outputColumn,concat(col(T.logic.split("\\|")(0)),lit(T.logic.split("\\|")(1))))
          case "ChangeDateFormat" => tempdf.withColumn(outputColumn,date_format(to_date(col(T.logic.split("\\|")(0)),T.logic.split("\\|")(1)),T.logic.split("\\|")(2)))
          case "AddPrefixIfNotNull" => tempdf.withColumn(outputColumn,
            when(col(T.dependentColumn)=== null || col(T.dependentColumn)==="",col(T.dependentColumn))
              .otherwise(concat(lit(T.logic),col(T.dependentColumn))))
          case "IfEqValueThenColElseCol" => tempdf.withColumn(outputColumn,
            when(col(T.logic.split("\\|")(0)) === T.logic.split("\\|")(1),col(T.logic.split("\\|")(0))
              .otherwise(col(T.logic.split("\\|")(2)))))
          case "FillNull" => tempdf.na.fill(T.logic.split("\\|")(0),Seq(T.logic.split("\\|")(1)))
          case "IfNullThenCol" => tempdf.withColumn("IfNullThenCol",
            when(col(T.logic.split("\\|")(0)) === null,col(T.logic.split("\\|")(1))))

        }
      }
    }

    df3.show


  }

}
