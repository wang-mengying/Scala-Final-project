package app
import bean.{Accident, Casualty, Vehicle}
import data.DataCleaning
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.{DecisionTreeRegressor, LinearRegression}
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.sql.{Dataset, _}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import utils.{DataSetGenerator, SparkSessionFactory}

object App {
  def main(args: Array[String]): Unit = {
//    Logger.getLogger("org").setLevel(Level.OFF)
    System.setProperty("hadoop.home.dir", "C:\\Program Files\\Hadoop");
    // Zookeeper host:port, group, kafka topic, threads
    //    if (args.length != 4) {
    //      println("Usage: StatStreamingApp <zkQuorum> <group> <topics> <numThreads>")
    //      System.exit(1)
    //    }
    // spark sql initialization
    // streaming preparation
    // val Array(zkQuorum, groupId, topics, numThreads) = args
    // val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    // val messages = KafkaUtils.createStream(streamingContext, zkQuorum, groupId, topicMap)
    // Streaming Data
    val spark = SparkSessionFactory.getSparkSession
    import spark.implicits._
    val streamingContext = new StreamingContext(spark.sparkContext, Seconds(5))
    DataSetGenerator.init
    var accidentDS: Dataset[Accident] = DataSetGenerator.accidentDS
    var vehicleDS: Dataset[Vehicle] = DataSetGenerator.vehicleDS
    var casualtyDS = DataSetGenerator.casualtyDS
    var joinedTable_train: Dataset[Row] = DataSetGenerator.trainSetJoinedGen
    var joinedTable_test: Dataset[Row] = DataSetGenerator.testSetJoinedGen
    var accident_train: Dataset[Accident] = DataSetGenerator.trainSetAccidentGen
    var accident_test: Dataset[Accident] = DataSetGenerator.testSetAccidentGen

    joinedTable_test.show(30)
//    joinedTable_train.show(30)

    val streamingData: ReceiverInputDStream[String] = streamingContext.socketTextStream("hadoop000", 9999)
    val input: DStream[Serializable] = streamingData.map(data => DataCleaning.parseData(data))
    input.foreachRDD(
      strs => {
        val data: Array[Serializable] = strs.collect()
        data.foreach(d => d match {
          case casualty: Casualty => {
            casualtyDS = casualtyDS.union(Seq(casualty).toDS())
          }
          case accident: Accident => {
            accidentDS = accidentDS.union(Seq(accident).toDS())
          }
          case vehicle: Vehicle => {
            vehicleDS = vehicleDS.union(Seq(vehicle).toDS())
            accidentDS.createOrReplaceTempView("accident")
            val sql = "select accident_index, accident_severity from accident"
            val accident_serverity: DataFrame = spark.sql(sql)
            joinedTable_train = accident_serverity.join(vehicleDS, "accident_index").join(casualtyDS, "accident_index")
            accident_serverity.orderBy("accident_index").show(1)
            println("Data Added")
            // data output - >   new csv. -> dataset
          }
          case _ => println("Data Parsing Failed")
        })
      }
    )
    streamingContext.start()
    streamingContext.awaitTermination()
  }

  def regression(path:String, labelName: String, featuresArray:Array[String] ) = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("final")
      .master("local[*]")
      .getOrCreate()

    val df = spark
      .read
      .option("header","true")
      .option("inferschema","true")
      .csv(path)

    var data = df.withColumnRenamed(labelName, "label")

    val featuresArray = featuresArray
    val assembler = new VectorAssembler().setInputCols(featuresArray).setOutputCol("features")

    val Array(training_lr, test_lr) = data.randomSplit(Array(0.9, 0.1),12)
    val Array(training_dt, test_dt) = data.randomSplit(Array(0.9, 0.1),12)

    val dt = new DecisionTreeRegressor()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setMaxBins(64)
      .setMaxDepth(15)

    val lr =new LinearRegression()
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setFitIntercept(true)
      .setMaxIter(20)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    val pipeline_dt = new Pipeline().setStages(Array(assembler, dt))
    val dtmodel = pipeline_dt.fit(training_dt)
    val predictions_dt = dtmodel.transform(test_dt)

    val pipeline_lr= new Pipeline().setStages(Array(assembler,lr))
    val lrModel = pipeline_lr.fit(training_lr)
    val predictions_lr = lrModel.transform(test_lr)

    val evaluator =new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("rmse")

    var matric_lr = evaluator.evaluate(predictions_lr)
    var matric_dt = evaluator.evaluate(predictions_dt)
    println(matric_lr)
    println(matric_dt)

    import org.apache.spark.ml.feature.SQLTransformer
    val sqlTrans = new SQLTransformer().setStatement(
      "SELECT *, SQRT(label) as label1 FROM __THIS__")

    val lr1 = new LinearRegression()
      .setFeaturesCol("features")
      .setLabelCol("label1")
      .setFitIntercept(true)

    val pipeline_lr1 = new Pipeline().setStages(Array(assembler,sqlTrans,lr1))

    val paramGrid = new ParamGridBuilder()
      .addGrid(lr1.elasticNetParam, Array(0.0, 0.8, 1.0))
      .addGrid(lr1.regParam,Array(0.1,0.3,0.5))
      .addGrid(lr1.maxIter, Array(20, 30))
      .build()

    val evaluator_lr1 =new RegressionEvaluator()
      .setLabelCol("label1")
      .setPredictionCol("prediction")
      .setMetricName("rmse")

    val trainValidation = new TrainValidationSplit()
      .setEstimator(pipeline_lr1)
      .setEvaluator(evaluator_lr1)
      .setEstimatorParamMaps(paramGrid)
      .setTrainRatio(0.8)

    val lrModel1 = trainValidation.fit(training_lr)

    lrModel1.getEstimatorParamMaps.foreach { println }
    lrModel1.getEvaluator.extractParamMap()
    lrModel1.getEvaluator.isLargerBetter

    val predictions_lr1 = lrModel1.transform(test_lr)
    val matric_lr1 = evaluator_lr1.evaluate(predictions_lr1)

    predictions_lr1.select("features","label","label1","prediction").show(5)

    println(matric_lr1)
  }
}





