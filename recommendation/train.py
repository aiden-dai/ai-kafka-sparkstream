from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.sql.types import StructType
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder


def train(data):

    als = ALS(userCol="user_id", itemCol="item_id", ratingCol="behavior_type",
              coldStartStrategy="drop")

    paramGrid = ParamGridBuilder() \
        .addGrid(als.rank, [5, 10]) \
        .addGrid(als.maxIter, [5, 10]) \
        .addGrid(als.regParam, [0.1, 0.01]) \
        .build()

    evaluator = RegressionEvaluator(metricName="rmse", labelCol="behavior_type",
                                    predictionCol="prediction")

    # Find best model
    crossval = CrossValidator(estimator=als,
                              estimatorParamMaps=paramGrid,
                              evaluator=evaluator,
                              numFolds=3)

    # Run cross-validation, and choose the best set of parameters.
    cv_model = crossval.fit(data)
    model = cv_model.bestModel

    metrics = cv_model.avgMetrics
    print(metrics)

    model.write().overwrite().save('data/als_model')


if __name__ == "__main__":

    spark = SparkSession.builder.appName('Recommendation').getOrCreate()

    schema = StructType()\
        .add("user_id", "integer")\
        .add("item_id", "integer")\
        .add("behavior_type", "integer")\
        .add('user_geohash', 'string')\
        .add("item_category", "integer")\
        .add("time", "string")

    df = spark.read.csv('data/sample.csv', header="true",
                        schema=schema).sort('time', ascending=True)
    # df = spark.read.csv('data/tianchi_mobile_recommend_train_user.csv', header="true", schema=schema)

    # Show schema
    df.printSchema()

    # Show total number of records
    count = df.count()
    print(count)

    # training = df.filter(df['time'].substr(0, 7) == '2014-11')
    # test = df.filter(df['time'].substr(0, 7) == '2014-12')
    # test.show()

    train(df)
