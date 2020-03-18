from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALSModel


def rec_for_user(model, user_id, size = 5):

    user_recs = model.recommendForAllUsers(size)
    user_recs.show(20, truncate=False)

    rec = user_recs.where(user_recs.user_id == u).select('recommendations.item_id', 'recommendations.rating').collect()
    print(rec)

    print(f'Top Items recommended for user {str(u)} are:')
    print(rec[0].item_id)
    for item in rec[0]['item_id']:
        print(f'Item: {item}')


if __name__ == "__main__":

    spark = SparkSession.builder.appName('Recommendation').getOrCreate()

    model = ALSModel.load('data/als_model')

    u = 95290487
    rec_for_user(model, u)
