package org.example
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.sum
object App {
  def main(args:Array[String]): Unit ={
    val spark = SparkSession
      .builder()
      .appName("Exp4")
      .master("yarn")
      .getOrCreate()
    import spark.implicits._

    val user_log = spark.read.option("header","true")
      .option("inferSchema","true")
      .csv("user_log_format1.csv")
    val result1=user_log.filter($"action_type">0).groupBy("item_id").count()
      .orderBy(-$"count").limit(100)   //最热门商品

    val user_info = spark.read.option("header","true").option("inferSchema","true")
      .csv("user_info_format1.csv")

    val result2=user_log.filter($"action_type">0).join(user_info,Seq("user_id"),"left")
      .select("merchant_id","age_range").na.drop("any")
      .filter($"age_range">0&&$"age_range"<4).groupBy("merchant_id").count()
      .orderBy(-$"count").limit(100) //最受年轻人欢迎的商家

    result1.show()

    result2.show()

    val result3=user_log.filter($"action_type"===2).join(user_info,Seq("user_id"),"left")
      .select("gender").na.drop("any").filter($"gender"<2).groupBy("gender").count()

    val sum3=result3.select(sum($"count")).head.getAs[Long]("sum(count)")//得到总数

    val result3_final=result3.select($"gender",($"count"/sum3).as("prob")) //购买商品的男女比例


    val result4=user_log.filter($"action_type"===2).join(user_info,Seq("user_id"),"left")
      .select("age_range").na.drop("any").filter($"age_range">0).groupBy("age_range").count()

    val sum4=result3.select(sum($"count")).head.getAs[Long]("sum(count)")//得到总数

    val result4_final=result4.select($"age_range",($"count"/sum4).as("prob")) //购买商品年龄比例


    result3_final.show()
    result4_final.show()

  }

}
