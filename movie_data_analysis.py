print("Hello World!!")

# The following codfe was run on GCP Data Proc - In Jupyter Notebook.

#Creating a Spark Session
from pyspark.sql import SparkSession

###### 1)  Initialize Spark Session #####
spark = SparkSession.builder \
    .appName("movies") \
    .getOrCreate()

######################## Data Load #########################
#### 2) Reading the Movies dataset.  #####
from pyspark.sql import types as T

hdfs_path = "/bootcamp/spark_assgn2/movies.csv"
header = True

movies_schema = T.StructType([
    T.StructField("MovieId", T.IntegerType(), False), 
    T.StructField("Title",  T.StringType(), False), 
    T.StructField("Genere", T.StringType(), False), 
])

movies_df = spark.read.csv(hdfs_path, header = header, schema = movies_schema)
#movies_df.show(5)
#movies_df.take(5)
#movies_df.show(5, truncate=False)
#movies_df.show(5, vertical=True)
movies_df.limit(5).toPandas()


##### 3) Reading the Ratings dataset.  #####
from pyspark.sql.functions import from_unixtime, to_timestamp, col
#from pyspark.sql import types as T

ratings_schema = T.StructType([
    T.StructField('UserId', T.IntegerType(), False), 
    T.StructField('MovieId', T.IntegerType(), False), 
    T.StructField('Rating', T.DecimalType(2,1), False), 
    T.StructField('TimeStamp', T.LongType(), False)
])
hdfs_path = "/bootcamp/spark_assgn2/ratings.csv"
header = True
ratings_df = spark.read.csv(hdfs_path, header = header, schema = ratings_schema).withColumn("RatingDateTime", to_timestamp(from_unixtime(col("TimeStamp"))) )

#Those numbers are usually an epoch timestamp, meaning the count of time elapsed since 1970-01-01 00:00:00 UTC. The most common forms are seconds, milliseconds, microseconds, or nanoseconds, and the exact unit depends on how many digits the number has and where it came from.
#How to identify it
#Around 10 digits usually means seconds since epoch.
#Around 13 digits usually means milliseconds since epoch.
#Around 16 digits usually means microseconds since epoch.
#Around 19 digits usually means nanoseconds since epoch.

#from_unixtime - Converts to Unix epoch seconds into a readable date-time string.
#to_timestamp() turns that readable string into a true timestamp column.

ratings_df.limit(5).toPandas()


#####  4) Reading Tags.csv file #####
tags_schema = T.StructType([
    T.StructField('UserId', T.IntegerType(), False), 
    T.StructField('MovieId', T.IntegerType(), False), 
    T.StructField('Tag', T.StringType(), False), 
    T.StructField('TimeStamp', T.LongType(), False)
])

hdfs_path = "/bootcamp/spark_assgn2/tags.csv"
header = True

tags_df = spark.read.csv(hdfs_path, header = header, schema = tags_schema) \
                     .withColumn('TagsDateTime', to_timestamp(from_unixtime(col("TimeStamp"))) ) 


tags_df.show(5, truncate = False)



################ Data Analytics #########################

####Problem Statement - 1 : Show the aggregated number of ratings per year. ###
from pyspark.sql.functions import col, year

ratings_df.groupBy(year(col('RatingDateTime')).alias('RatingYear')) \
          .count().orderBy(col('RatingYear').asc()). \
           show()


##### Problem Statement 2 - Show the average monthly number of ratings ####
from pyspark.sql.functions import month, avg

"""
#Find the number of ratings for each month
monthly_rating_df = ratings_df.groupBy(year(col('RatingDateTime')).alias('RatingYear'), month(col('RatingDateTime')).alias('RatingMonth') ) \
                              .count()

#Find the overall average
monthly_rating_df.agg(avg(col('count'))).alias('monthly_avg_rating').show()
"""

monthly_rating_df = ratings_df.groupBy(year(col('RatingDateTime')).alias('RatingYear'), month(col('RatingDateTime')).alias('RatingMonth') ) \
                              .agg(avg(col('Rating')))

monthly_rating_df.orderBy(col('RatingYear').desc(), col('RatingMonth').desc() ).show()


#### Problem Statement 3 - Show the rating levels distribution ####
#ratings_df.show(5)
from pyspark.sql import functions as F
from pyspark.sql import Window

window_spec = Window.partitionBy()

"""
ratings_df.groupBy(col('Rating')) \
          .count() \
          .orderBy(col('Rating')) \
          .show() """

result = (ratings_df
    .withColumn('Distribution', 
        F.when(F.col('Rating') <= 2.0, '0.0 - 2.0')
        .when((F.col('Rating') > 2.0) & (F.col('Rating') <= 4.0), '2.0 - 4.0')
        .otherwise('> 4.0')
    )
    .groupBy('Distribution')
    .count()
    .withColumn('overall_total', 
        (F.col('count') / F.sum(F.col('count')).over(window_spec)) * 100) )

result.show()


#### Problem Statement 4 - Show the 18 movies that are tagged but not rated ####

#ratings_df.show(5)
#tags_df.show(5)

rating_tags_joined_df = tags_df.alias('tag').join(ratings_df.alias('ratng'), (col('tag.MovieId') == col('ratng.MovieId')), how = 'left') \
                               .filter(col('ratng.MovieId').isNull()).select(col('tag.movieId'))
#& ( col('tag.UserId') == col('ratng.UserId')

result = rating_tags_joined_df.alias('rt').join(movies_df.alias('m'), (col('rt.MovieId') == col('m.MovieId')), how = 'inner') \
                     .select(col('rt.MovieId'), col('m.Title')).distinct()

result.select(col('movieId'), col('Title')).limit(20).toPandas()



#### Problem Statement 5 - Show the movies that have rating but no tag ####
rating_tags_joined_df = ratings_df.alias('ratng').join(tags_df.alias('tag'), (col('tag.MovieId') == col('ratng.MovieId')), how = 'left') \
                                  .filter(col('tag.MovieId').isNull()) \
                                  .select(col('ratng.movieId'), col('ratng.UserId'), col('ratng.Rating'))
#& ( col('tag.UserId') == col('ratng.UserId')
#ratng.UserId - Is used for problem 6

result = rating_tags_joined_df.alias('rt').join(movies_df.alias('m'), (col('rt.MovieId') == col('m.MovieId')), how = 'inner') \
                     .select(col('rt.MovieId'), col('m.Title')).distinct()

result.select(col('movieId'), col('Title')).limit(20).toPandas()


#### Problem Statement - 6 : Focusing on the rated untagged movies with more than 30 user ratings,
# show the top 10 movies in terms of average rating and number of ratings
from pyspark.sql.functions import count_distinct, count,rank
from pyspark.sql import Window 

#window_spec = Window.partitionBy("RatingYear").orderBy(F.col("avg_rating").desc())
window_spec = Window.orderBy(col("avg_rating").desc(), col("no_of_ratings").desc())

result = rating_tags_joined_df.alias('rt').join( movies_df.alias('m'), (col('rt.MovieId') == col('m.MovieId')), how = 'inner' ) \
                              .select( col('rt.MovieId'), col('m.Title'), col('rt.UserId'), col('rt.Rating') ) \
                              .distinct() \
                              .groupBy( col('rt.MovieId'), col('m.Title') ) \
                              .agg( avg(col('rt.Rating')).alias('avg_rating'), \
                                    count(col('rt.Rating')).alias('no_of_ratings'), \
                                    count_distinct(col('rt.UserId')).alias('user_count') \
                                  ) \
                              .filter( col('user_count') > 30 ) \
                              .withColumn('RN', rank().over(window_spec)) \
                              .filter( col('RN') <= 10 ) \
                              .select( col('MovieId'), col('Title'), col('avg_rating'), col('no_of_ratings') )

                   
result.show()


#### Problem 7 : What is the average number of tags per movie in tagsDF? And the
#average number of tags per user? How does it compare with the
#average number of tags a user assigns to a movie? ####

#tags_df.show(5)

#Avg num of tags per movie. 
tags_df.groupBy(col('MovieId')) \
                               .agg(count(col('Tag')).alias('no_of_tags')) \
                               .agg(avg(col('no_of_tags')).alias('avg_tag_per_movie')) \
                               .show()

#Avg num of tags per user. 
tags_df.groupBy(col('UserId')) \
                               .agg(count(col('Tag')).alias('no_of_tags')) \
                               .agg(avg(col('no_of_tags')).alias('avg_tag_per_user')) \
                               .show()
"""
#Avg num of tags a user assigns to a movie.
tags_df.groupBy(col('UserId'), col('MovieId')) \
                               .agg( count_distinct(col('Tag')).alias('no_of_tags') ) \
                               .agg( avg(col('no_of_tags')).alias('avg_tag_per_user_movie') ) \
                               .show()  """



#### Problem 8 - Identify the users that tagged movies without rating them ####

result = tags_df.alias('t').join(ratings_df.alias('r'),  ( col('t.MovieId') == col('r.MovieId') )  ,how = 'left' ) \
                .filter(col('r.UserId').isNull()).select(col('t.UserId')).distinct()
result.show()

#( col('t.UserId') == col('r.UserId') ) & 


result = tags_df.alias('t').join(ratings_df.alias('r'), ( col('t.UserId') == col('r.UserId') ) &  ( col('t.MovieId') == col('r.MovieId') )  ,how = 'left' ) \
                .filter(col('r.UserId').isNull()).distinct()   #select(col('t.UserId')).
result.show()


#### Problem 9 - What is the average number of ratings per user in ratings DF? And the average number of ratings per movie? ####
from pyspark.sql.functions import count, avg

#Part - 1 : Average number of ratings per user in ratings DF
# Step 1: Correct the alias placement
results = ratings_df.groupBy(col('UserId')).agg(count(col('Rating')).alias('no_of_ratings'))

# Step 2: Now 'no_of_ratings' exists, so we can average it
final_avg = results.agg(avg(col('no_of_ratings')))

final_avg.show()

#Part - 2 : Average number of ratings per movie
results = ratings_df.groupBy(col('MovieId')).agg(count(col('Rating')).alias('no_of_ratings'))

results.agg(avg(col('no_of_ratings'))).show()


#### Problem 10 - What is the predominant (frequency based) genre per rating level? ####

#ratings_df.show(5)
#movies_df.show(5, truncate = False)

from pyspark.sql import functions as F
from pyspark.sql import Window

window_spec = Window.orderBy(col('no_of_rating').desc())

# Wrapping in ( ) removes the need for \
result = ( movies_df.alias('mv') \
    .join(ratings_df.alias('rt'), ( F.col('mv.MovieId') == F.col('rt.MovieId') ), how = 'inner') \
    .select( F.col('mv.MovieId'), F.col('mv.Title'), F.col('mv.Genere'), F.col('rt.Rating') ) \
    .withColumn('ExplodedGenre', F.explode(F.split(F.col('mv.Genere'), '\\|'))) ) \
    .groupBy(col('ExplodedGenre')) \
    .agg( F.count(col('Rating')).alias('no_of_rating') ) \
    .withColumn('RN', F.rank().over(window_spec)) \
    .filter(col('RN') == 1)

result.show()


#### Problem 11 - What is the predominant tag per genre and the most tagged genres? ####
from pyspark.sql import Window
from pyspark.sql import functions as F

window_spec = Window.partitionBy(col('ExplodedGenre'), col('Tag')).orderBy(col('no_of_tags').desc()) 

#Predominant Tag per Genere
result1 = ( movies_df.alias('mv') \
    .join(tags_df.alias('t'), ( F.col('mv.MovieId') == F.col('t.MovieId') ), how = 'inner') \
    .select( F.col('mv.MovieId'), F.col('mv.Title'), F.col('mv.Genere'), F.col('t.Tag') ) \
    .withColumn('ExplodedGenre', F.explode(F.split(F.col('mv.Genere'), '\\|'))) ) \
    .groupBy(col('ExplodedGenre'), col('Tag') ) \
    .agg( F.count(col('Tag')).alias('no_of_tags') ) \
    .withColumn('rk', F.rank().over(window_spec))


#result.orderBy(col('rk').desc()).show() - Uncomment this. 

#Most Tagged Genere
window_spec2 = Window.orderBy(col('no_of_tags').desc()) 

result2 = ( movies_df.alias('mv') \
    .join(tags_df.alias('t'), ( F.col('mv.MovieId') == F.col('t.MovieId') ), how = 'inner') \
    .select( F.col('mv.MovieId'), F.col('mv.Title'), F.col('mv.Genere'), F.col('t.Tag') ) \
    .withColumn('ExplodedGenre', F.explode(F.split(F.col('mv.Genere'), '\\|'))) ) \
    .groupBy(col('ExplodedGenre')) \
    .agg( F.count(col('Tag')).alias('no_of_tags') ) \
    .withColumn('rk', F.rank().over(window_spec2))

result2.filter(col('rk') == 1).show()


#### Problem 12 - What are the most predominant (popularity based) movies? - Most seen by user ####
from pyspark.sql import Window
from pyspark.sql.functions import col

window_spec = Window.orderBy(col('no_of_users').desc())

results = (ratings_df.alias('rt')
    .join(movies_df.alias('mv'), F.col('rt.MovieId') == F.col('mv.MovieId'), how='inner')
    .groupBy(F.col('mv.Title'), F.col('mv.MovieId'))
    .agg(F.count(F.col('rt.UserId')).alias('no_of_users'))
    .withColumn('rk', F.rank().over(window_spec))
)

results.filter(col('rk') < 10).orderBy(col('rk')).show()


#### Problem 13 : Top 10 movies in terms of average rating (provided more than 30 users reviewed them) ####
from pyspark.sql.functions import rank

window_spec = Window.orderBy(col('avg_rating').desc())

result = ratings_df.alias('rt') \
                   .join(movies_df.alias('mv'), col('rt.MovieId') == col('mv.MovieId'), how = 'inner') \
                   .groupBy(col('mv.MovieId'), col('mv.Title') ) \
                   .agg(avg(col('rt.Rating')).alias('avg_rating'), count(col('rt.UserId')).alias('no_of_users') ) \
                   .filter(col('no_of_users') > 30) \
                   .withColumn('rk', rank().over(window_spec))
        
result.filter(col('rk') < 10).show()





