# Databricks notebook source
# MAGIC %md
# MAGIC # Exploring reddit data using Spark
# MAGIC 
# MAGIC This notebook provides startup code for downloading Reddit data from the Azure Blob Storage bucket specifically setup for this project.   
# MAGIC _<b>Make sure you are running this notebook on a cluster which has the credentials setup to access Azure Blob Storage, otherwise this notebook will not be able to read the data!</b>_
# MAGIC 
# MAGIC _<b>Make sure you are using DataBricks Runtime 11.3 or newer otherwise you will not be able to save any new files in this repository!</b>_
# MAGIC 
# MAGIC The dataset for this notebook is described in [The Pushshift Reddit Dataset](https://arxiv.org/pdf/2001.08435.pdf) paper.

# COMMAND ----------

# MAGIC %md
# MAGIC # Instruction
# MAGIC YouTube and Twitch: The online video heavyweight battle of the century.
# MAGIC 
# MAGIC While there are many similarities between Twitch and YouTube - for example, they are both audio/video sharing platforms - there are also many differences between the two platforms.
# MAGIC 
# MAGIC The most significant difference between Twitch and YouTube is the viewer's interest. Twitch caters to live-stream hosts and is the largest live-streaming platform in the world. On the other hand, YouTube uses an excellent search engine to help those interested in a specific topic find videos that match their heart's expectations.
# MAGIC 
# MAGIC While the two video sites have different focuses, they also have similar modules. YouTube, for example, has a live streaming system, while Twitch is also drawing on more different categories of video submissions.
# MAGIC 
# MAGIC This study examines the difference in users' focus using these two video platforms by comparing two different subreddit statements on Reddit, 'youtube' and 'Twitch'.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Propose 10 different avenues of analysis for your data.
# MAGIC 
# MAGIC ### 1. What is the total performance of users’ comments on Twitch and Youtube in recent 2 years?
# MAGIC ##### Business Goal: 
# MAGIC Python and Spark would be the primary programming languages used to compare the total comments analysis between Twitch and Youtube users. By doing this, readers can therefore get an overview of the total users activities between these two social media platforms from 2021/01/01 to 2022/08/31. 
# MAGIC ##### Technical Proposal：
# MAGIC Technically, packages of seaborn and matplotlib would be adapted to vividly show visualizations. The new datasets of ‘submissions_twitch’, ‘submissions_youtube’ are generated based on 15 variables such as ‘author_id’, ‘title’, and so on. Two bar plots would therefore be generated with the columns of ‘num_comments’ in these two new datasets. 
# MAGIC 
# MAGIC ### 2. What’s the Time series patterns of comments number. Based on Q1 
# MAGIC ##### Business Goal: 
# MAGIC To define time series patterns for Twitch comments and Youtube comments. 
# MAGIC ##### Technical Proposal: 
# MAGIC Use “plotly” to do line plots based on date and ‘num_comments’, and combine the Twitch plots with Youtube plots. Find whether there are the same patterns between Twitch comments and Youtube comments. Then analyze the user activity behavior. And try to track the influence of those big news (some may match with the summits in plots) 
# MAGIC 
# MAGIC 
# MAGIC ### 3. What’s the relationship between length of comments and “score”(upvote-downvote)?
# MAGIC ##### Business Goal: 
# MAGIC Determine if long comments could represent high quality comments somehow. 
# MAGIC ##### Technical Proposal: 
# MAGIC Create a new column called “body_length”, which saves numeric data representing the length of comments. Create a scatter plot that compares the body_length and score. (Here we define that a high quality comment doesn't  have to be right or receive a positive score, instead as long as it could activate people to give responses and even write down sub-comments. Thus a very low negative score also can be considered a high quality comments. )
# MAGIC 
# MAGIC ### 4. What’s the ratio of having outer links in Youtube and Twitch? Are comments with an outer link are more likely to have a higher score?
# MAGIC ##### Business Goal: 
# MAGIC Determine whether having outer links means giving higher quality comments. And analyze the outer links ratio in these two platforms.
# MAGIC ##### Technical Proposal: 
# MAGIC Choose “is_self” as a new feature. Generate two pie charts for the ratio of having outer links in Youtube and Twitch, and then compare and analyze the result. Build another viz that contains the information of “is_self” and “score”, find whether these two features have any correlation to each other.
# MAGIC 
# MAGIC ### 5. What’s the hottest topics in Youtube and Twitch Reddits?
# MAGIC ##### Business goal: 
# MAGIC To define the most popular topics for Youtube and Twitch lovers. Dive deeper into user hobbies in social media.
# MAGIC ##### Technical Proposal: 
# MAGIC Select the “body” column in “comments_youtube” and “comments_twitch” table, use “Wordcloud”, “matplotlib”, “pandas'' to generate word clouds and word frequency lists for Youtube and Twitch comments. Implement key words analysis. Compare their word clouds viz and frequency lists, and find their differences and similarities.
# MAGIC 
# MAGIC ### 6. What are the top 10 qualified topics that users agree most for both Youtube and Twitch? Based on Q5. 
# MAGIC ##### Business Goal: 
# MAGIC The goal is to discover the topic for youtube and twitch which receive the top 10 of reviewing scores correspondingly. This would reflect the users’ acceptance level for each topic. Python and Spark would be the primary programming languages used for this business goal. 
# MAGIC ##### Technical Proposal: 
# MAGIC The column of ‘score’ from the submissions_youtube and submissions_twitch would be used. Since the score column is created by subtracting the downvotes from upvotes, the scores would be balanced to 0 if the topic is too controversial or not influential at all. The quantitative analysis would be conducted to filter topics with the top 10 highest reviewing scores. 
# MAGIC 
# MAGIC ### 7. What are the similarities and differences of the topic that is detected over 18 or below 18 on twitch and youtube? Based on Q5.
# MAGIC ##### Business Goal: 
# MAGIC The goal is to discover the insights of the functions of platforms that users care about based on whether the comment is over 18 or not. The programming languages would be Spark and Python. 
# MAGIC ##### Technical Proposal: 
# MAGIC Based on the business goal of getting the most popular topic based on users’ ages. The NLP tool of Text Classification analysis can be conducted. For plotting, the table of ‘submissions_twitch’ and ‘‘submissions_youtube’ and column of ‘over_18’ would be used; the machine learning methods of decision trees might also 
# MAGIC used to classify the content that a specific topic has the most. 
# MAGIC 
# MAGIC ### 8. How to help the company predict the future over-18 comments and posts? Based on Q7.
# MAGIC ##### Business Goal: 
# MAGIC The goal is to improve the overall quality of the subreddit platform, and therefore, improve the efficiency for administrators when dealing with posts. The programming languages used would be Python and Spark. 
# MAGIC ##### Technical Proposal: 
# MAGIC The table of ‘submissions_twitch’ and ‘submissions_youtube’ would be used. And the columns of ‘title’, ‘selftext’, and ‘over_18’ can be used. Since titles are all text data, the method of vectorization should be applied first. Then, the training and testing dataset would be splitted for doing the predictive models. For prediction, the NLP model of text prediction can be used to help the administrator automatically determine if the text is over-18 and needs to be deleted. 
# MAGIC 
# MAGIC ### 9. Sentiments analysis for Youtube and Twitch Reddits? Based on Q5.
# MAGIC ##### Business Goal: 
# MAGIC Analyze redditers’ sentiments score and make comparison between these two video based platforms.
# MAGIC ##### Technical Proposal: 
# MAGIC Apply “nltk” package in the “body” column to perform data tokenization, lementization, stopword removal. Pretrained models will be used to inference text sentiment. Comparisons based on sentiment and interactive features including scores and number of comments will be drawn.
# MAGIC 
# MAGIC ### 10. Provide a list of technical problems, what is the frequency that users have on these technical problems of each platform? / Any suggestions for twitch and youtube to improve based on users’ post? 
# MAGIC ##### Business Goal: 
# MAGIC The goal of this business question is to help twitch and youtube discover the most frequent problems that users meet, and help them to improve the user experience. The programming language used for this question would be Python and Spark. 
# MAGIC ##### Technical Proposal: 
# MAGIC In order to filter the topics, a list of technical problems such as ‘appcrash’ and ‘server collapse’ needs to be collected first. TFIDF would be used to deal with the text data.  After cleaning the dataset, the NLP method of Keyword Extraction can be conducted to extract the topic word and fit the problems to posts. 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Data From DBFS

# COMMAND ----------

# MAGIC %md
# MAGIC ### Submission Data

# COMMAND ----------

submissions_new=spark.read.parquet("/FileStore/data/submissions_new.parquet")
submissions_new.show()

# COMMAND ----------

submissions_new.count()

# COMMAND ----------

submissions_nona = submissions_new.filter(~submissions_new["title"].rlike("\[deleted by user\]"))

# COMMAND ----------

submissions_nona = submissions_nona.filter(~submissions_nona["selftext"].rlike("\[removed\]"))

# COMMAND ----------

submissions_nona.show()

# COMMAND ----------

submissions_nona.count()

# COMMAND ----------

# dbutils.fs.rm("/FileStore/data/submissions_nona.parquet", True)

# COMMAND ----------

submissions_nona.write.parquet("/FileStore/data/submissions_nona.parquet") 

# COMMAND ----------

submissions_nona =spark.read.parquet("/FileStore/data/submissions_nona.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Comments Data

# COMMAND ----------

import pyspark.sql.functions as f

# COMMAND ----------

comments =spark.read.parquet("/FileStore/data/comments_all.parquet")

# COMMAND ----------

comments = comments.withColumn('created_utc', f.from_unixtime("created_utc", "MM/dd/yyyy"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Remove the comments that has been deleted and NA value

# COMMAND ----------

comments_nona = comments.filter(~comments["body"].rlike("ed\]"))

# COMMAND ----------

comments_nona = comments_nona.na.drop(subset=["body"])

# COMMAND ----------

comments_nona.show()

# COMMAND ----------

comments_nona.count()

# COMMAND ----------

# dbutils.fs.rm("/FileStore/data/comments_nona.parquet", True)

# COMMAND ----------

comments_nona.write.parquet("/FileStore/data/comments_nona.parquet") 

# COMMAND ----------

# MAGIC %md
# MAGIC #EDA 

# COMMAND ----------

import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA1: Number of Submissions in these two subreddit(bar chart)

# COMMAND ----------

number_sub = submissions_nona.groupby('subreddit').count()
number_sub.show()

# COMMAND ----------

df_numbersub = number_sub.cache().toPandas()
df_numbersub

# COMMAND ----------

# MAGIC %md
# MAGIC There are 163,584 youtube and 97,243 Twitch submissions. The bar plot clearly shows the difference between the popularity of submissions in these two subreddits.

# COMMAND ----------

plt.figure(figsize=(10, 8))
df_numbersub.plot.bar(x='subreddit', y='count') 
plt.title("The popularity of Submissions by subreddits")
plt.ylabel("Number of Submissions") 
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC The bar plot shows the polulation of submissions in these two different subreddit. It is obvious that youtube had more submission then Twitch from January 1, 2021 to Auguest 21, 2022.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comparing Trend of Submission Activities with Parent Company Stock Price

# COMMAND ----------

number_sub_byday = submissions_new.groupby("created_utc", "subreddit").count().toPandas()

# COMMAND ----------

number_sub_byday["created_utc"] = pd.to_datetime(number_sub_byday["created_utc"])

# COMMAND ----------

stock_GOOG = pd.read_csv("/dbfs/FileStore/data/GOOG.csv")

# COMMAND ----------

stock_AMZN = pd.read_csv("/dbfs/FileStore/data/AMZN.csv")

# COMMAND ----------

stock = stock_GOOG[["Date", "Adj Close"]]
stock.columns = ["Date", "GOOG"]

# COMMAND ----------

stock = stock.merge(stock_AMZN[["Date", "Adj Close"]], on="Date")

# COMMAND ----------

stock.columns = ["Date", "GOOG", "AMZN"]
stock["Date"] = pd.to_datetime(stock["Date"])

# COMMAND ----------

fig, axes = plt.subplots(figsize=(16,8))
sns.set_theme(style='darkgrid')  # 'whitegrid', 'dark', 'white', 'ticks'
ax1 = sns.lineplot(x = 'Date',
                   y = 'AMZN',
                   label = 'AMZN',
                   color = 'r',
                   data = stock).set(title='Submission Activities Overlayed with Daily Closing Stock Price of Parent Comapines', ylabel='Stock Price')
ax2 = sns.lineplot(x = 'Date',
                   y = 'GOOG',
                   label = 'GOOG',
                   color = 'g',
                   data = stock)
plt.legend(title='parent company', loc='upper left')
ax3 = plt.twinx()
sns.lineplot(x='created_utc',
             y='count',
             hue='subreddit',
             data=number_sub_byday).set(ylabel='submissions')

plt.xticks(rotation = 45)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC After coverting the 'created_utc' column to datetime, we plotted the number of submissions for each subreddit per day. We can see that overall the daily activities for both subreddits are declining over the past two years. For most of times YouTube subreddit have more daily submissions then Twitch subreddit, while occasional spikes for YouTube are more significant, usually indicating a service interruption (which will be discussed later). The stock price time plot for the parent company of both platforms are included in the following chunks for reference. It shows that the overall trend of the submission activities in these two subreddits does not reflect the stock price of the two companies, certain “pikes” in the capital market does correlates with more discussions on reddit.

# COMMAND ----------

# MAGIC %md
# MAGIC #NLP

# COMMAND ----------

from pyspark.sql import SparkSession
from sparknlp.pretrained import PretrainedPipeline
import sparknlp
from sparknlp.base import *
from sparknlp.annotator import *
import pyspark.sql.functions as f
from pyspark.ml.feature import CountVectorizer, IDF, HashingTF, SQLTransformer
# Start Spark Session with Spark NLP
# spark = sparknlp.start()

# COMMAND ----------

# Start Spark Session with Spark NLP
spark = SparkSession.builder \
        .appName("SparkNLP") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.jars.packages", "com.johnsnowlabs.nlp:spark-nlp_2.12:4.2.1") \
    .master('yarn') \
    .getOrCreate()

# COMMAND ----------

spark

# COMMAND ----------

# MAGIC %md
# MAGIC ##  Basic Data Text Checks 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clean your text data
# MAGIC stop words, stemming, lemmatizing, removing unusual characters, matching synonyms, etc (at least 5)

# COMMAND ----------

def nlp_pipline(input_colname, input_df, tokenize=True, normalize=True, lemmatize=True, stemming=False, stopwords=True, tf=False, idf=False, vivekn=False, Word2Vec=False, finisher=True, embed_finisher=False):
    documentAssembler = DocumentAssembler().setInputCol(input_colname).setOutputCol("document")
#     num_outputs = sum([tokenize, normalize, lemmatize, stopwords, vivekn, finisher])
    pipline_used = [documentAssembler]
    i = 0
    if tokenize:
        tokenizer = Tokenizer().setInputCols(["document"]).setOutputCol('output' + str(i+1))
        i += 1
        pipline_used.append(tokenizer)
    if normalize:
        normalizer = Normalizer().setInputCols(['output' + str(i)]).setOutputCol('output' + str(i+1)).setLowercase(True).setCleanupPatterns(["""[^\w\d\s]"""]) 
        pipline_used.append(normalizer)
        i += 1
    if lemmatize:
        lemmatizer = LemmatizerModel.pretrained().setInputCols('output' + str(i)).setOutputCol('output' + str(i+1))
        pipline_used.append(lemmatizer)
        i += 1
    if stemming:
        stemmer = Stemmer().setInputCols('output' + str(i)).setOutputCol('output' + str(i+1))
        pipline_used.append(lemmatizer)
        i += 1
    if stopwords:
        stopwords_cleaner = StopWordsCleaner().pretrained("stopwords_en", "en").setInputCols('output' + str(i)).setOutputCol('output' + str(i+1)).setCaseSensitive(False)
        pipline_used.append(stopwords_cleaner)
        i += 1
    if vivekn:
        vivekn =  ViveknSentimentModel.pretrained().setInputCols(["document", 'output' + str(i)]).setOutputCol('output' + str(i+1))
        pipline_used.append(vivekn)
        i += 1
    if Word2Vec:
        embeddings = Word2VecModel.pretrained().setInputCols(['output' + str(i)]).setOutputCol('output' + str(i+1))
        pipline_used.append(embeddings)
        i += 1
    if finisher:
        finisher = Finisher().setInputCols('output' + str(i)).setOutputCols('output' + str(i+1)).setCleanAnnotations(False).setOutputAsArray(True)
        pipline_used.append(finisher)
        i += 1
    if embed_finisher:
        embeddingsFinisher = EmbeddingsFinisher().setInputCols(['output' + str(i)]).setOutputCols('output' + str(i+1)).setOutputAsVector(True)
        pipline_used.append(embeddingsFinisher)
        i += 1
    if tf:
        tf = CountVectorizer(inputCol=('output' + str(i)), outputCol=('output' + str(i+1)))
        i += 1
    if idf:
        idf = IDF(inputCol=('output' + str(i)), outputCol=('output' + str(i+1)))
        i += 1
        
    pipeline = Pipeline().setStages(pipline_used).fit(input_df)
    result = pipeline.transform(input_df)

    return result, 'output' + str(i)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Most common words for submissions

# COMMAND ----------

result, output_colname = nlp_pipline("title", submissions_nona, tokenize=True, normalize=True, lemmatize=True, stopwords=True, vivekn=False, Word2Vec=False, finisher=False, embed_finisher=False)
result.selectExpr(str(output_colname) + ".result")

# COMMAND ----------

result_mcw = result.withColumn('word', f.explode(f.col(str(output_colname) + ".result"))).groupBy('subreddit', 'word').count().sort('count', ascending=False)
result_mcw.show(truncate=False)

# COMMAND ----------

result_mcw.where(f.col("subreddit") == "youtube").show(10)

# COMMAND ----------

result_mcw.where(f.col("subreddit") == "Twitch").show(10)

# COMMAND ----------

mcw_df_title = result_mcw.limit(20).toPandas()
plt.figure(figsize=(16, 8))
sns.set(font_scale=1.5)
sns.barplot(data=mcw_df_title, x="word", y="count", hue="subreddit").set(title='Most Common Words for Submission Titles')
plt.xticks(rotation=45)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC The most common words (except thier name) for titles are video and stream. Essentially Twitch is a live streaming platform and youtube is a video sharing platform. Therefore the most comment word for Twitch is "stream" and for youtube is "video". Twitch is focus on the quality of live streaming, and youtube is more concerned about the quality of videos uploaded by users.

# COMMAND ----------

result2, output_colname2 = nlp_pipline("selftext", submissions_nona, tokenize=True, normalize=True, lemmatize=True, stopwords=True, vivekn=False, Word2Vec=False, finisher=False, embed_finisher=False)
result2.selectExpr(str(output_colname2) + ".result").show(truncate=True)

# COMMAND ----------

result_text_mcw = result2.withColumn('word', f.explode(f.col(str(output_colname2) + ".result"))).groupBy('subreddit', 'word').count().sort('count', ascending=False)
result_text_mcw.show(truncate=False)

# COMMAND ----------

mcw_df_text = result_text_mcw.limit(20).toPandas()
plt.figure(figsize=(16, 8))
sns.set(font_scale=1.5)
sns.barplot(data=mcw_df_text, x="word", y="count", hue="subreddit").set(title='Most Common Words for Submission Bodies')
plt.xticks(rotation=45)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC The most common word for the body of submission are "stream" and "video" seperately, which are same as the most common word for title. 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Most common words for comments

# COMMAND ----------

result_comments, colname_comments = nlp_pipline("body", comments_nona, tokenize=True, normalize=True, lemmatize=True, stopwords=True, finisher=False)
result_comments.selectExpr(str(colname_comments) + ".result").show(truncate=False)

# COMMAND ----------

result_comments_mcw = result_comments.withColumn('word', f.explode(f.col(str(colname_comments) + ".result"))).groupBy('subreddit', 'word').count().sort('count', ascending=False)
result_comments_mcw.show(truncate=False)

# COMMAND ----------

result_comments_mcw.where(f.col("subreddit") == "youtube").show(10)

# COMMAND ----------

result_comments_mcw.where(f.col("subreddit") == "Twitch").show(10)

# COMMAND ----------

mcw_df_body = result_comments_mcw.limit(20).toPandas()
plt.figure(figsize=(16, 8))
sns.set(font_scale=1.3)
sns.barplot(data=mcw_df_body, x="word", y="count", hue="subreddit").set(title='Most Common Words for Comments')
plt.xticks(rotation=45)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### What is the distribution of text lengths?

# COMMAND ----------

submissions_nona.show(5)

# COMMAND ----------

number_text = submissions_new.groupby('len_title').count()
## order by len_title
number_text.orderBy(col("len_title").asc(),col("count").asc()).show(truncate=False)

# COMMAND ----------

bins, counts = submissions_new.select('len_title').rdd.flatMap(lambda x: x).histogram(100)
plt.figure(figsize=(8,6))
plt.hist(bins[:-1], bins=bins, weights=counts)
plt.title("The distribution of text lengths")
plt.ylabel("count of text lengths") 
plt.xlabel("text lengths") 
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Standard Pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC ### What are important words according to TF-IDF?

# COMMAND ----------

documentAssembler = DocumentAssembler().setInputCol("title").setOutputCol("document")
tokenizer = Tokenizer().setInputCols(["document"]).setOutputCol("token").fit(submissions_nona)
normalizer = Normalizer().setInputCols(['token']).setOutputCol('normalized').setLowercase(True).setCleanupPatterns(["""[^\w\d\s]"""])  # Removes punctuations and special characters
lemmatizer = LemmatizerModel.pretrained().setInputCols(['normalized']).setOutputCol('lemma')
stopwords_cleaner = StopWordsCleaner().pretrained("stopwords_en", "en").setInputCols(['lemma']).setOutputCol('clean_lemma').setCaseSensitive(False)
finisher = Finisher().setInputCols(['clean_lemma']).setOutputCols('token_features').setCleanAnnotations(False).setOutputAsArray(True) 
# hashingTF = HashingTF(inputCol="token_features", outputCol="tf_features", numFeatures=100)
tf = CountVectorizer(inputCol='token_features', outputCol='tf_features')
idf = IDF(inputCol='tf_features', outputCol='tf_idf_features')

# explodeVectors = SQLTransformer(statement=
#       "SELECT EXPLODE(tf_idf_features) AS features")
explodeVectors = SQLTransformer(statement=
      "SELECT regexp_extract(tf_idf_features, r'\],\[(.*?)\]', 1) AS features, * FROM __THIS__")
pipeline = Pipeline().setStages([documentAssembler, tokenizer, normalizer, lemmatizer, stopwords_cleaner, finisher, tf, idf]).fit(submissions_nona)
tfidf_ytb = pipeline.transform(submissions_nona.where(f.col("subreddit") == "youtube"))
# res_tfidf, output_colnam_name = nlp_pipline("selftext", submissions_nona, tokenize=True, normalize=True, lemmatize=True, stopwords=True, vivekn=False, finisher=True)

# COMMAND ----------

tfidf_twc = pipeline.transform(submissions_nona.where(f.col("subreddit") == "Twitch"))

# COMMAND ----------

# combine the result and score (contained in keywords.metadata)
#scores = result_tfidf \
#    .selectExpr(f"explode(arrays_zip(output6.result, output6.metadata)) as resultTuples") \
#    .selectExpr("resultTuples['0'] as keyword", "resultTuples['1'].score as score")

# COMMAND ----------

tfidf_ytb.select('tf_idf_features').show(5)

# COMMAND ----------

tfidf_ytb = tfidf_ytb.withColumn("tfidf_features",tfidf_ytb.tf_idf_features.cast('string'))
tfidf_twc = tfidf_twc.withColumn("tfidf_features",tfidf_twc.tf_idf_features.cast('string'))

# COMMAND ----------

tfidf_ytb.createOrReplaceTempView('tfidf_df_ytb')
tfidf_twc.createOrReplaceTempView('tfidf_df_twc')

# COMMAND ----------

result_importance_ytb = spark.sql(r'''
    SELECT
        regexp_extract(tfidf_features, r'\],\[(.*?)\]', 1) as features, token_features
    FROM tfidf_df_ytb
    ''')
result_importance_twc = spark.sql(r'''
    SELECT
        regexp_extract(tfidf_features, r'\],\[(.*?)\]', 1) as features, token_features
    FROM tfidf_df_twc
    ''')

# COMMAND ----------

from pyspark.ml.functions import array_to_vector
df_importance_ytb = result_importance_ytb.select(split(col("features"),",").alias("featureArray"), 'token_features')
df_importance_twc = result_importance_twc.select(split(col("features"),",").alias("featureArray"), 'token_features')

# COMMAND ----------

df_importance_ytb.show(5)

# COMMAND ----------

# combine the result and score (contained in keywords.metadata)
scores_ytb = df_importance_ytb.selectExpr(f"explode(arrays_zip(token_features, featureArray)) as resultTuples").selectExpr("resultTuples['token_features'] as words", "resultTuples['featureArray'] as score").withColumn("subreddit", f.lit("youtube"))
scores_twc = df_importance_twc.selectExpr(f"explode(arrays_zip(token_features, featureArray)) as resultTuples").selectExpr("resultTuples['token_features'] as words", "resultTuples['featureArray'] as score").withColumn("subreddit", f.lit("Twitch"))

# COMMAND ----------

# Ranked in decending order, as higher scores means higher importance
scores_ytb = scores_ytb.orderBy(col("score").desc())
scores_ytb.show(10, truncate = False)

# COMMAND ----------

# MAGIC %md
# MAGIC From the TFIDF score we can see the F-word rules supreme in the Youtube subreddit. Adult contents are more prevalent in this subreddit compared to Twitch. Keywords include "video", "hotel", "meditation", "montage" clearly shows user preference and attention.

# COMMAND ----------

scores_twc = scores_twc.orderBy(col("score").desc())
scores_twc.show(10, truncate = False)

# COMMAND ----------

tfidf_all = scores_ytb.union(scores_twc).orderBy(col("score").desc())

# COMMAND ----------

tfidf_all_plot = tfidf_all.limit(20).toPandas().astype({'score':'float'})
tfidf_all_plot

# COMMAND ----------

plt.figure(figsize=(16, 8))
sns.set(font_scale=1.3)
sns.barplot(data=tfidf_all_plot, x="words", y="score", hue="subreddit").set(title='TFIDF Score for Submission Title')
plt.xticks(rotation=45)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Twitch subreddit users clearly pays more attention to technical contents and interaction with other users. Several trending twitch channel made it in the top of this list.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Identify important keywords for your reddit data and use regex searches to create at least two dummy variables to identify comments of particular topics.

# COMMAND ----------

from pyspark.sql.functions import when
comments_nona_dummy = comments_nona.withColumn('ads_dummy', when(col("body").rlike("ads|advertis|sponsor"), 1).otherwise(0))
comments_nona_dummy.groupby("subreddit", "ads_dummy").count().show()

# COMMAND ----------



# COMMAND ----------

# comments_nona_dummy = comments_nona.withColumn('error_dummy', when(col("body").rlike("crash|sound|speed|cookies|quality|corrupt|error|wrong"), 1).otherwise(0))

comments_nona_dummy = comments_nona.withColumn("error_dummy", when(col("body").rlike("crash|corrupt|collapse"),"crash/corrupt")
                                 .when(col("body").rlike("(no|echo|miss|delay).*(sound|audio)"),"audio")
                                 .when(col("body").rlike("speed|wait.*buffer"),"speed")
                                 .when(col("body").rlike("(low|bad).*quality"),"quality")
                                 .when(col("body").rlike("ads|advertis|sponsor"),"ads")
                                 .when(col("body").rlike("cookies"),"cookies")
                                 .otherwise("normal"))

comments_error_df = comments_nona_dummy.groupby("subreddit", "error_dummy").count().orderBy(col("subreddit").asc(),col("count").desc()).cache().toPandas()

# COMMAND ----------

comments_error_df

# COMMAND ----------

comments_error_df2 = comments_error_df[comments_error_df['error_dummy']!="normal"]
plt.figure(figsize=(16, 8))
sns.set(font_scale=2)
sns.barplot(data=comments_error_df2, x="error_dummy", y="count", hue="subreddit").set(title='Technical Problems of Each Platform', xlabel='Error Type')
plt.xticks(rotation=45)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## sentiment model
# MAGIC Build at least one sentiment model using the sparkNLP framework. Pick a pre-trained model or train your model! You can start off with simple pos/neg/neu sentiment. Maybe you will want to build your own textual classification model.... that is completely up to you and your topical interests and technical goals. You must report a table of summary statistics from any model(s) leveraged.

# COMMAND ----------

## Sentiment Analysis (Vivekn Model) 
## reference link of Vivekn Sentiment Approach: https://nlp.johnsnowlabs.com/2021/11/22/sentiment_vivekn_en.html
import sparknlp
from sparknlp.base import *
from sparknlp.annotator import *
from pyspark.ml import Pipeline
from pyspark.sql.functions import *

# COMMAND ----------

#Sentiment Analysis on "submissions"   
sentiment_result_submissions, sentiment_colname_submissions = nlp_pipline("title", submissions_nona, tokenize=True, normalize=True, lemmatize=True, stopwords=True, vivekn=True, Word2Vec=False, finisher=True, embed_finisher=False)
sentiment_result_submissions = sentiment_result_submissions.select(col(sentiment_colname_submissions).alias("final_sentiment_submissions"), "subreddit","score_dummy","title_dummy","over_18","is_self")
sentiment_result_submissions.show(truncate=False)

# COMMAND ----------

#Sentiment Analysis on "comments"   
sentiment_result_comments, sentiment_colname_comments = nlp_pipline("body", comments_nona, tokenize=True, normalize=True, lemmatize=True, stopwords=True, vivekn=True, Word2Vec=False, finisher=True, embed_finisher=False)
sentiment_result_comments = sentiment_result_comments.select(col(sentiment_colname_comments).alias("final_sentiment_comments"),"subreddit", "score")
sentiment_result_comments.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC # 3 graphs, 3 tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sentiment Analysis on "submissions" 

# COMMAND ----------

## summary statistics table for youtube subreddit:
sentiment1_youtube = sentiment_result_submissions.select("final_sentiment_submissions", "subreddit","score_dummy","title_dummy").where(f.col("subreddit") == "youtube")

sentiment1_youtube.summary().show() 

# COMMAND ----------

## summary statistics table for Twitch subreddit:
sentiment1_twitch = sentiment_result_submissions.select("final_sentiment_submissions","subreddit","score_dummy","title_dummy").where(f.col("subreddit") == "Twitch")
sentiment1_twitch.summary().show()

# COMMAND ----------

# MAGIC %md
# MAGIC We can see from the tables above that on average Twitch subreddit's submissions enjoys more upvotes and are more likely to contain a question compared to youtube subreddit.

# COMMAND ----------

sentiment_result_submissions.stat.crosstab("score_dummy","title_dummy").show() 

# COMMAND ----------

## Youtube: summary of Cross Tabulation (Contingency Table): 
model1_contingency = sentiment1_youtube.stat.crosstab("score_dummy","final_sentiment_submissions")
model1_contingency.show() 
sentiment_result_table_youtube = model1_contingency.cache().toPandas()


## Twitch: summary of Cross Tabulation (Contingency Table): 
model1_contingency = sentiment1_twitch.stat.crosstab("score_dummy","final_sentiment_submissions")
model1_contingency.show() 
sentiment_result_table_twitch = model1_contingency.cache().toPandas()

# COMMAND ----------

plt.figure(figsize=(10, 8))
sentiment_result_table_youtube.plot.bar(x='score_dummy_final_sentiment_submissions', y='WrappedArray(negative)' )
plt.title("The relationship between the quality of a post and the post content(positive) for Youtube")
plt.xlabel("If the post research a general aggrement") 
plt.ylabel("Totol Counts")

plt.figure(figsize=(10, 8))
sentiment_result_table_youtube.plot.bar(x='score_dummy_final_sentiment_submissions', y='WrappedArray(positive)' )
plt.title("The relationship between the quality of a post and the post content(positive) for Youtube")
plt.xlabel("If the post research a general aggrement") 
plt.ylabel("Totol Counts")

plt.show()

sentiment_result_table_twitch.plot.bar(x='score_dummy_final_sentiment_submissions', y='WrappedArray(negative)',color="pink" )
plt.title("The relationship between the quality of a post and the post content(positive) for Twitch")
plt.xlabel("If the post research a general aggrement") 
plt.ylabel("Totol Counts")

plt.figure(figsize=(10, 8))
sentiment_result_table_twitch.plot.bar(x='score_dummy_final_sentiment_submissions', y='WrappedArray(positive)',color="pink" )
plt.title("The relationship between the quality of a post and the post content(positive) for Twitch")
plt.xlabel("If the post research a general aggrement") 
plt.ylabel("Totol Counts")

plt.show()



# COMMAND ----------

## Youtube: summary of Cross Tabulation (Contingency Table): 
model1_contingency2 = sentiment1_youtube.stat.crosstab("title_dummy","final_sentiment_submissions") 
model1_contingency2.show() 
sentiment_youtube_table2 = model1_contingency2.cache().toPandas()


## Twitch: summary of Cross Tabulation (Contingency Table): 
model1_contingency2 = sentiment1_twitch.stat.crosstab("title_dummy","final_sentiment_submissions") 
model1_contingency2.show() 
sentiment_twitch_table2 = model1_contingency2.cache().toPandas()

# COMMAND ----------

## youtube

plt.figure(figsize=(10, 8))
sentiment_youtube_table2.plot.bar(x='title_dummy_final_sentiment_submissions', y='WrappedArray(negative)',color="blue" )
plt.title("Youtube: The relationship between the title of a post and the post content(negative)")
plt.xlabel("If the title has a question format") 
plt.ylabel("Total Counts")

plt.figure(figsize=(10, 8))
sentiment_youtube_table2.plot.bar(x='title_dummy_final_sentiment_submissions', y='WrappedArray(positive)' ,color="blue" )
plt.title("Youtube: The relationship between the title of a post and the post content(positive)")
plt.xlabel("If the title has a question format") 
plt.ylabel("Total Counts")
plt.show() 

## twitch 

plt.figure(figsize=(10, 8))
sentiment_twitch_table2.plot.bar(x='title_dummy_final_sentiment_submissions', y='WrappedArray(negative)',color="pink" )
plt.title("Twitch: The relationship between the title of a post and the post content(negative)")
plt.xlabel("If the title has a question format") 
plt.ylabel("Total Counts")


plt.figure(figsize=(10, 8))
sentiment_twitch_table2.plot.bar(x='title_dummy_final_sentiment_submissions', y='WrappedArray(positive)' ,color="pink" )
plt.title("Twitch: The relationship between the title of a post and the post content(positive)")
plt.xlabel("If the title has a question format") 
plt.ylabel("Total Counts")
plt.show() 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sentiment Analysis on "comments" 

# COMMAND ----------

## summary statistics table :
sentiment_result_comments.select("final_sentiment_comments","score").summary().show()

# COMMAND ----------

sentiment_result_comments = sentiment_result_comments.sort("score")
bins, counts = sentiment_result_comments.select('score').rdd.flatMap(lambda x: x).histogram(100) #divided into 100 groups
plt.figure(figsize=(8,6))
plt.hist(bins[:-1], bins=bins, weights=counts)
plt.title("Distribution of Scores in 'comments_nano' Dataset")
plt.xlabel("Comments Score") 
plt.ylabel("Number of Comments Score")
plt.show()

# COMMAND ----------

#Based on the previous table and Visualization, we decided to separate the dataset into three parts: score below 0, score equal to 0 and score over 0.
comments_score_below = sentiment_result_comments.filter(sentiment_result_comments.score < 0)
comments_score_equal = sentiment_result_comments.filter(sentiment_result_comments.score == 0)
comments_score_over = sentiment_result_comments.filter(sentiment_result_comments.score > 0)

# COMMAND ----------

comments_score_below_gb = comments_score_below.groupby('final_sentiment_comments','subreddit').count().sort("subreddit").cache()
comments_score_below_gb.toPandas()

# COMMAND ----------

comments_score_equal_gb = comments_score_equal.groupby('final_sentiment_comments','subreddit').count().sort("subreddit").cache()
comments_score_equal_gb.toPandas()

# COMMAND ----------

comments_score_over_gb = comments_score_over.groupby('final_sentiment_comments','subreddit').count().sort("subreddit").cache()
comments_score_over_gb.toPandas()

# COMMAND ----------

#https://spark.apache.org/docs/latest/api/python/reference/pyspark.pandas/api/pyspark.pandas.DataFrame.plot.bar.html
positive = [8749, 4025, 10893, 8167, 254604, 194019]
negative = [8004, 3263, 9105, 5919, 176108, 125797]
na = [2787, 1545, 3393, 2992, 57715, 47488]
blank = [523, 307, 751, 794, 11427, 10408]
index = ['Twitch_below_0', 'Youtube_below_0', 'Twitch_equal_0', 'Youtube_equal_0', 'Twitch_over_0', 'Youtube_over_0']
df = pd.DataFrame({'positive': positive,
                   'negative': negative,
                   'na':na,
                   'blank':blank}, index=index)

ax = df.plot.bar(title=" Comments' Score Distribution for Youtube and Twitch", figsize=(12, 7))
ax.set_xlabel("Score in different situations")
ax.set_ylabel("Comments Number")
plt.xticks(rotation=45)
fig.show()

# COMMAND ----------

data = {'below_positive':8749,
        'below_negative':8004,
        'below_na':2787,
        'below_blank':523,}
colors = ['orange', 'green', 'cyan', 'skyblue']
explode = (0.1, 0.1, 0.1, 0.1)
plt.figure(figsize=(6, 6))
plt.pie(list(data.values()), explode=explode, labels=data.keys(), colors=colors, autopct='%2.1f%%', shadow=True, startangle=90)
plt.title("Twitch Comments Distribution Where Score Below 0")
plt.show()

data = {'below_positive':4025,
        'below_negative':3263,
        'below_na':1545,
        'below_blank':307,}
colors = ['orange', 'green', 'cyan', 'skyblue']
explode = (0.1, 0.1, 0.1, 0.1) 
plt.figure(figsize=(6, 6))
plt.pie(list(data.values()), explode=explode, labels=data.keys(), colors=colors, autopct='%2.1f%%', shadow=True, startangle=90)
plt.title("Youtube Comments Distribution Where Score Below 0")
plt.show()


# COMMAND ----------

data = {'equal_positive':10893,
        'equal_negative':9105,
        'equal_na':3393,
        'equal_blank':751,}
colors = ['orange', 'green', 'cyan', 'skyblue']
explode = (0.1, 0.1, 0.1, 0.1) 
plt.figure(figsize=(6, 6))
plt.pie(list(data.values()), explode=explode, labels=data.keys(), colors=colors, autopct='%2.1f%%', shadow=True, startangle=90)
plt.title("Twitch Comments Distribution Where Score Equal to 0")
plt.show()

data = {'equal_positive':8167,
        'equal_negative':5919,
        'equal_na':2992,
        'equal_blank':794,}
colors = ['orange', 'green', 'cyan', 'skyblue']
explode = (0.1, 0.1, 0.1, 0.1) 
plt.figure(figsize=(6, 6))
plt.pie(list(data.values()), explode=explode, labels=data.keys(), colors=colors, autopct='%2.1f%%', shadow=True, startangle=90)
plt.title("Youtube Comments Distribution Where Score Equal to 0")
plt.show()


# COMMAND ----------

data = {'over_positive':254604,
        'over_negative':176108,
        'over_na':57715,
        'over_blank':11427,}
colors = ['orange', 'green', 'cyan', 'skyblue']
explode = (0.1, 0.1, 0.1, 0.1) 
plt.figure(figsize=(6, 6))
plt.pie(list(data.values()), explode=explode, labels=data.keys(), colors=colors, autopct='%2.1f%%', shadow=True, startangle=90)
plt.title("Twitch Comments Distribution Where Score Over 0")
plt.show()

data = {'over_positive':194019,
        'over_negative':125797,
        'over_na':47488,
        'over_blank':10408,}
colors = ['orange', 'green', 'cyan', 'skyblue']
explode = (0.1, 0.1, 0.1, 0.1) 
plt.figure(figsize=(6, 6))
plt.pie(list(data.values()), explode=explode, labels=data.keys(), colors=colors, autopct='%2.1f%%', shadow=True, startangle=90)
plt.title("Youtube Comments Distribution Where Score Over 0")
plt.show()
