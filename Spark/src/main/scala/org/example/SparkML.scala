package org.example
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler, VectorIndexer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, countDistinct, lit, max, min, sum, to_date}
import org.apache.spark.sql.types.{LongType, TimestampType}
import org.apache.spark.ml.linalg.Vector


object SparkML {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Exp4")
      .master("yarn")
      .getOrCreate()
    import spark.implicits._

    val user_log = spark.read.option("header", "true")
      .option("inferSchema", "true")
      .csv("user_log_format1.csv")

    val user_info = spark.read.option("header", "true").option("inferSchema", "true")
      .csv("user_info_format1.csv")

    val user_test = spark.read.option("header", "true").option("inferSchema", "true")
      .csv("test_format1.csv").withColumn("source", lit(1))

    val user_train = spark.read.option("header", "true").option("inferSchema", "true")
      .csv("train_format1.csv").withColumn("source",lit(0))

    val user_all = user_train.union(user_test)

    // 将年龄进行one-hot编码
    val age_one_hot = "user_id" +: Range(0, 9).map(t => "CASE WHEN age_range='" + t + "' THEN 1 ELSE 0 END AS age" + t)
    // 将性别进行one-hot编码
    val gender_one_hot = Range(0, 3).map(t => "CASE WHEN gender='" + t + "' THEN 1 ELSE 0 END AS gender" + t)
    // 将操作进行one-hot编码
    val action_one_hot = "*" +: Range(0, 4).map(t => "CASE WHEN action_type='" + t + "' THEN 1 ELSE 0 END AS action" + t)

    val value_map = Map("age_range" -> 0, "gender" -> 2)
    val user_info_one_hot = user_info.na.fill(value_map).selectExpr((age_one_hot ++ gender_one_hot): _*)

    val user_log_one_hot=user_log.selectExpr(action_one_hot: _*) .withColumn("time_stamp",
      to_date(col("time_stamp").cast("string"), "MMdd").cast(TimestampType).cast(LongType))

    val user_group =user_log_one_hot.groupBy("user_id").agg(
      count("*").as("user_action_count"),
      countDistinct("item_id").as("user_item_count"),
      countDistinct("cat_id").as("user_cat_count"),
      countDistinct("merchant_id").as("user_merchant_count"),
      countDistinct("brand_id").as("user_brand_count"),
      ((max("time_stamp")-min("time_stamp"))/3600).as("user_time"), //转成小时数，购买时间差
      sum("action0").as("user_click"),
      sum("action1").as("user_cart"),
      sum("action2").as("user_purchase"),
      sum("action3").as("user_add")
    )


   val merchant_group=user_log_one_hot.groupBy("merchant_id").agg(
      count("*").as("mer_action_count"),
     countDistinct("item_id").as("mer_item_count"),
     countDistinct("cat_id").as("mer_cat_count"),
     countDistinct("user_id").as("mer_user_count"),
     countDistinct("brand_id").as("mer_brand_count"),
     sum("action0").as("mer_click"),
     sum("action1").as("mer_cart"),
     sum("action2").as("mer_purchase"),
     sum("action3").as("mer_add")
   )

    val mer_user_group=user_log_one_hot.groupBy("merchant_id","user_id" ).agg(
      count("*").as("joint_action_count"),
      countDistinct("item_id").as("joint_item_count"),
      countDistinct("cat_id").as("joint_cat_count"),
      countDistinct("brand_id").as("joint_brand_count"),
      ((max("time_stamp")-min("time_stamp"))/3600).as("joint_time"), //转成小时数，购买时间差
      sum("action0").as("joint_click"),
      sum("action1").as("joint_cart"),
      sum("action2").as("joint_purchase"),
      sum("action3").as("joint_add")
    )

    val data = user_all.join(user_group,Seq("user_id"),"left")
      .join(merchant_group,Seq("merchant_id"),"left")
      .join(mer_user_group,Seq("user_id","merchant_id"),"left")
      .withColumn("user_rate", col("user_purchase")/col("user_click"))//计算用户的购买/点击比
       .withColumn("mer_rate",col("mer_purchase")/col("mer_click"))//计算商家的购买点击比
      .withColumn("joint_rate",col("joint_purchase")/col("joint_click"))
      .withColumn("label",col("label").cast("Double"))
      .na.fill(0)

    val featuresCol=data.columns.filter(t=>t!="source"&&t!="label")

    //把特征值转成向量
    val assembler=new VectorAssembler()
      .setInputCols(featuresCol)
      .setOutputCol("features")
    val transform_data= assembler.transform(data)


    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(transform_data)
    // Automatically identify categorical features, and index them.
    // Set maxCategories so features with > 4 distinct values are treated as continuous.
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4)
      .fit(transform_data)

    val trainData =transform_data.filter($"source"===0).select("label","features")
    val testData=transform_data.filter($"source"===1).select("label","features")

    // Train a RandomForest model.
    val rf = new RandomForestClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
      .setNumTrees(10)

    // Convert indexed labels back to original labels.
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    // Chain indexers and forest in a Pipeline.
    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, rf, labelConverter))

    // Train model. This also runs the indexers.
    val model = pipeline.fit(trainData)

    // Make predictions.
    val predictions = model.transform(testData).select("label","features","predictedLabel","probability")

    val target= predictions.rdd.map{t=>
      val label=t.getAs[Double]("label")
      val features=t.getAs[Vector]("features")
      val user_id=features.apply(0)
      val merchant_id=features.apply(1)
      val probability =t.getAs[Vector]("probability")
      val probability1=probability.apply(0)
      val probability2=probability.apply(1)
      var prob_final=0.0
      if(label==1) prob_final=probability1
      else prob_final=probability2
      (user_id.toInt,merchant_id.toInt,prob_final)
    }.toDF("user_id","merchant_id","prob")
    //根据预测结果获取信息，包括计算出的概率

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val outputPathStr = "hdfs://localhost:9000/user/root/predict"
    val outputPath = new Path(outputPathStr)
    if (fs.exists(outputPath))
      fs.delete(outputPath, true)
    target.coalesce(1).write.option("header", "true").csv(outputPathStr)

  }
}
