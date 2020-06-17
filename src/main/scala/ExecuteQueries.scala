import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.databricks.spark.xml._
import org.apache.hadoop.mapreduce.lib.input.InvalidInputException

object ExecuteQueries {
  var noOfQueries=0
  def main(args: Array[String]):Unit={
    val spark: SparkSession=SparkSession.builder()
      .master("local[1]")
      .appName("Execute Queries on XML")
      .getOrCreate()

    try {
      val xmlDataframe = spark.read.option("rowTag", "user").xml("src/main/xmldata/users.xml")
      xmlDataframe.write.mode("append").parquet("src/main/parquetdata")

      renameParquet(spark)

      queryOverParquet(spark)

      renameOutput(spark)
    }
    catch{
      case ex: InvalidInputException =>{
        println("Exception "+ex)
      }
    }
  }

  def renameParquet(spark: SparkSession): Unit = {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val basePath = "src/main/parquetdata/"
    val fileName=fs.globStatus(new Path(basePath+"part*"))(0).getPath.getName
    fs.rename(new Path(basePath+fileName), new Path(basePath+"users.parquet"))
  }

  def queryOverParquet(spark: SparkSession): Unit= {
    val parquetDF = spark.read.parquet("src\\main\\parquetdata\\users.parquet");
    parquetDF.show()
    parquetDF.createOrReplaceTempView("parquetFile")

    var outputDF=spark.sql("SELECT id, salary from parquetFile where salary>100000")
    output(outputDF)

    outputDF=spark.sql("SELECT id, salary from parquetFile where salary<40000")
    output(outputDF)

    outputDF=spark.sql("select id, first_name from parquetFile where first_name like 'F%'")
    output(outputDF)

    outputDF=spark.sql("SELECT country, count(*) AS c from parquetFile group by country having c>1 order by c desc")
    output(outputDF)

    outputDF=spark.sql("SELECT id, title from parquetFile where title!=''")
    output(outputDF)

    outputDF=spark.sql("select id, last_name from parquetFile where last_name like 'S%'")
    output(outputDF)

    outputDF=spark.sql("select id, email from parquetFile where email like '%.edu'")
    output(outputDF)
}

  def output(outputDF: DataFrame): Unit = {
    noOfQueries+=1
    outputDF.coalesce(1).write.mode("append").option("header", "true").csv("src/main/output")
  }

  def renameOutput(spark: SparkSession): Unit = {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val basePath = "src/main/output/"
    var i=0;
    for(i<- 1 to noOfQueries) {
      val fileName=fs.globStatus(new Path(basePath+"part*"))(0).getPath.getName

      fs.rename(new Path(basePath + fileName), new Path(basePath + i+".csv"))
    }
  }
}