import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._


object Interview {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName("Interview").getOrCreate()
    spark.sparkContext.setLogLevel("Error")

    val df = spark.read
      .option("header", true)
      .option("sep", ",")
      .option("charset", "UTF-8")
      .csv("sample.csv")

    val pullnumeric = udf {
      (input: String) => {
        input.split("-")(1)
      }
    }

    val numbered_date = udf {
      (input: String) =>
        val date_array = input.split("-")
        date_array.map(_.toInt).sum
    }

    // column rename
    df.withColumnRenamed("AccountingDate", "Date").show()
    // copy one column to other column
    df.withColumn("NewColumn", col("AccountingDate")).show()
    // changing date format
    df.withColumn("AccountingDate", date_format(to_date(col("AccountingDate")), "MM/dd/yyyy")).show
    // pull numeric part
    df.withColumn("ERPCommodityIdNumeric_part", pullnumeric(col("ERPCommodityId"))).show
    // date sum == dd+mm+yyyy
    df.withColumn("NumberedDate", numbered_date(col("AccountingDate"))).show()

    // group by
    df.select("*").groupBy("InvoiceLineNumber").agg(sum(col("Quantity")).as("Sum")).show()

    // select all columns
    df.select("*").show

    // replace a character in all the columns
    val columns = df.columns
    val df2 = columns.foldLeft(df) { (tempdf, column_name) => {
            tempdf.withColumn(column_name, regexp_replace(col(column_name),"-", "\"")).withColumn(column_name,regexp_replace(col(column_name),"\"","\"\"\""))

      }
    }
    df2.write.option("header","true").option("charset","UTF-8").option("escape", "\"").mode(SaveMode.Overwrite).csv("D:/csv_output/temp_test")
  }

}
