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

submissions_nona=spark.read.parquet("/FileStore/data/submissions_nona.parquet")
submissions_nona.show()

# COMMAND ----------

submissions_nona.count()

# COMMAND ----------

submissions_nona.filter(submissions_nona.score!=0).count()

# COMMAND ----------

# dbutils.fs.rm("/FileStore/data/submissions_nona.parquet", True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Comments Data

# COMMAND ----------

comments_nona =spark.read.parquet("/FileStore/data/comments_nona.parquet")

# COMMAND ----------

comments_nona.show()

# COMMAND ----------

comments_nona.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Time Series

# COMMAND ----------

number_sub_byhour = pd.read_csv("/dbfs/FileStore/data/number_sub_byhour.csv")

# COMMAND ----------

number_sub_byhour.head()

# COMMAND ----------

# MAGIC %md
# MAGIC # Machine Learning

# COMMAND ----------

from pyspark.sql import SparkSession
from sparknlp.pretrained import PretrainedPipeline
import sparknlp
from sparknlp.base import *
from sparknlp.annotator import *
import pyspark.sql.functions as f
from pyspark.sql.functions import when, col
from pyspark.ml.feature import CountVectorizer, IDF, HashingTF, SQLTransformer
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from sparknlp.pretrained import PretrainedPipeline
from pyspark.ml.linalg import Vectors
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
# MAGIC ##  Note
# MAGIC * 2 transformations
# MAGIC * Split data into testing and training data if you are running supervised models.
# MAGIC * answer at least two different business questions (more is always fine!)
# MAGIC * Within each analysis, you will compare the performance of two or more models types by selecting at least two different hyperparameter sets OR at least two different pretrained models. 
# MAGIC * At least one of these model analyses must leverage Spark MLlib models
# MAGIC * Run model evaluation metrics (ROC AUC, Confusion Matrix, Accuracy, R2, etc.) on your models and/or hyperparameter options.
# MAGIC * Evaluate your models using at least two different metrics and compare and interpret your results, for each ML analysis.
# MAGIC * Prepare at least 1 appropriate table and at least 1 chart for each ML analysis. You do not need one for every model / hyperparameter set
# MAGIC * You must save at least one ML model into DBFS. Demonstrate that you can save your best ML model and load the prepared model into your notebook and start making predictions without any additional training. This can be done in a separate notebook.
# MAGIC * Pipeline bonus: (+4 points): Apply pipelines to your ML analysis so that you can compare hyperparameters and model options without having to re-run large parts of code. Be sure to clearly indicate in your notebook and on your website the code you are implementing for the bonus.

# COMMAND ----------

# MAGIC %md
# MAGIC # Executive Summary

# COMMAND ----------

# MAGIC %md
# MAGIC ## Classification Model

# COMMAND ----------

comments_nona.filter(comments_nona["score"] == 1).count()

# COMMAND ----------

comments_Score = comments_nona.withColumn('score_level', when((col("score")>1) & (col("score")<8), "Somewhat agree")
                                                        .when((col("score")<-1) & (col("score")>-8), "Somewhat disagree")
                                                        .when(col("score")>=8, "Strongly agree")
                                                        .when(col("score")<=-8, "Strongly disagree")
                                                        .otherwise("Neither agree nor disagree"))

# COMMAND ----------

score_leveldf = comments_Score.groupby("score_level","subreddit").count().orderBy(col("subreddit").asc(),col("count").desc()).cache().toPandas()

# COMMAND ----------

score_leveldf["proportion"] = score_leveldf["count"] / score_leveldf.groupby('subreddit')['count'].transform('sum')

# COMMAND ----------

score_leveldf

# COMMAND ----------

display(score_leveldf)

# COMMAND ----------

plt.figure(figsize=(12, 7))
sns.set(font_scale=1.3)
sns.barplot(data=score_leveldf, x="score_level", y="proportion", hue="subreddit").set(title='Attitude of Comments for Each Platform', xlabel='Attitude', ylabel='Proportation')
plt.xticks(rotation=20)
plt.show() 

# COMMAND ----------

# MAGIC %md
# MAGIC The score in the comment has been divided into five different levels: Strongly agree (score is greater than 7), Somewhat agree (score is between 2 to 7), neither agree nor disagree (score is between -1 to 1), somewhat disagree (score is between -7 to -2) and strongly disagree (score is smaller than -7). Since the comments population differs from youtube and Twitch, the graph above shows the proportion for each group in different subreddit. The majority remains indifferent (neither agree nor disagree). Those who really care are more inclined to give upvotes. Youtube users tend to have a consensus for solid agreements.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Train test split

# COMMAND ----------

# MAGIC %md
# MAGIC The data is imbalance, we will apply the weight into RandomForestClassifier. PySpark needs to have a weight assigned to each instance (i.e., row) in the training set. The team create a mapping to apply a weight to each training instance.

# COMMAND ----------

import numpy as np
stringIndexer_attitude = StringIndexer(inputCol="score_level", outputCol="agree_level")
Score_df = stringIndexer_attitude.fit(comments_Score).transform(comments_Score)
y_collect = Score_df.select("score_level").groupBy("score_level").count().collect()
unique_y = [x["score_level"] for x in y_collect]
total_y = sum([x["count"] for x in y_collect])
unique_y_count = len(y_collect)
bin_count = [x["count"] for x in y_collect]
class_weights_spark = {i: ii for i, ii in zip(unique_y, total_y / (unique_y_count * np.array(bin_count)))}

# COMMAND ----------

Score_df.show(5)

# COMMAND ----------

class_weights_spark

# COMMAND ----------

from itertools import chain
mapping_expr = f.create_map([f.lit(x) for x in chain(*class_weights_spark.items())])
comments_Score = comments_Score.withColumn("weight", mapping_expr.getItem(col("score_levbel")))

# COMMAND ----------

comments_Score.show(5)

# COMMAND ----------

train_data, test_data, predict_data = comments_Score.randomSplit([0.8, 0.18, 0.02], 24)

# COMMAND ----------

print("Number of training records: " + str(train_data.count()))
print("Number of testing records : " + str(test_data.count()))
print("Number of prediction records : " + str(predict_data.count()))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create pipeline and train a model

# COMMAND ----------

from pyspark.ml.feature import OneHotEncoder, StringIndexer, IndexToString, VectorAssembler, RegexTokenizer, StopWordsRemover, CountVectorizer, Normalizer, IndexToString
from pyspark.ml.classification import RandomForestClassifier, LogisticRegression, DecisionTreeClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml import Pipeline, Model

# COMMAND ----------

# MAGIC %md
# MAGIC Since the training data is the body of comments, text data need to be vectorized before modeling. The team applied StringIndexer for the target variable which change different level into numerical value. After tokenization and removing stopwords, CountVectorizer were applied for the tokenized results.

# COMMAND ----------

stringIndexer_attitude = StringIndexer(inputCol="score_level", outputCol="agree_level")
regex_tokenizer = RegexTokenizer(inputCol="body", outputCol="words", pattern="\\W")
remover = StopWordsRemover(inputCol="words", outputCol="filtered")
cv = CountVectorizer(inputCol="filtered", outputCol="features")
normalizer = Normalizer(inputCol='features', outputCol= "features_norm", p=1.0)
labelConverter = IndexToString(inputCol="prediction", 
                               outputCol="predictedattitude", 
                               labels = ['Neither agree nor disagree', 'Somewhat agree', 'Strongly agree', 'Somewhat disagree', 'Strongly disagree'])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Random Forest Model Training

# COMMAND ----------

rf = RandomForestClassifier(labelCol="agree_level", featuresCol="features_norm", numTrees=60, maxDepth=7,  weightCol="weight")
pipeline = Pipeline(stages=[stringIndexer_attitude, regex_tokenizer, remover, cv, normalizer, rf, labelConverter])

# Fit the pipeline to training documents.
model_rf1 = pipeline.fit(train_data)

# COMMAND ----------

model_rf1.save("/FileStore/data/rf1")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Random Forest Model Testing

# COMMAND ----------

predictions_train = model_rf1.transform(train_data)
evaluatorRF = MulticlassClassificationEvaluator(labelCol="agree_level", predictionCol="prediction", metricName="accuracy")
accuracy = evaluatorRF.evaluate(predictions_train)

print("Accuracy = %g" % accuracy)
print("Train Error = %g" % (1.0 - accuracy))

# COMMAND ----------

predictions_rf1 = model_rf1.transform(test_data)
evaluatorRF = MulticlassClassificationEvaluator(labelCol="agree_level", predictionCol="prediction", metricName="accuracy")
accuracy_rf = evaluatorRF.evaluate(predictions_rf1)

print("Accuracy = %g" % accuracy_rf)
print("Test Error = %g" % (1.0 - accuracy_rf))

# COMMAND ----------

evaluatorRF = BinaryClassificationEvaluator(labelCol="agree_level", rawPredictionCol="prediction", metricName="areaUnderROC")
roc_result_rf = evaluatorRF.evaluate(predictions)
print("Test ROC = %g" % roc_result_rf)

# COMMAND ----------

rf2 = RandomForestClassifier(labelCol="agree_level", featuresCol="features_norm", numTrees=30, maxDepth=7,  weightCol="weight")
pipeline_rf = Pipeline(stages=[stringIndexer_attitude, regex_tokenizer, remover, cv, normalizer, rf2, labelConverter])

# Fit the pipeline to training documents.
model_rf = pipeline_rf.fit(train_data)

# COMMAND ----------

model_rf.save("/FileStore/data/rf2")

# COMMAND ----------

prediction_rf = model_rf.transform(test_data)
evaluatorRF = MulticlassClassificationEvaluator(labelCol="agree_level", predictionCol="prediction", metricName="accuracy")
accuracy_rf2 = evaluatorRF.evaluate(prediction_rf)

print("Accuracy = %g" % accuracy_rf2)
print("Test Error = %g" % (1.0 - accuracy_rf2))

# COMMAND ----------

evaluatorRF = BinaryClassificationEvaluator(labelCol="agree_level", rawPredictionCol="prediction", metricName="areaUnderROC")
roc_result_rf2 = evaluatorRF.evaluate(prediction_rf)
print("Test ROC = %g" % roc_result_rf2)

# COMMAND ----------

lr = LogisticRegression(labelCol="agree_level", featuresCol="features_norm", maxIter=100) 
pipeline_lr = Pipeline(stages=[stringIndexer_attitude, regex_tokenizer, remover, cv, normalizer, lr, labelConverter])

# Fit the pipeline to training documents.
model_lr = pipeline_lr.fit(train_data)

# COMMAND ----------

model_lr.save("/FileStore/data/lr")

# COMMAND ----------

prediction_lr = model_lr.transform(test_data)
evaluatorLR = MulticlassClassificationEvaluator(labelCol="agree_level", predictionCol="prediction", metricName="accuracy")
accuracy_lr = evaluatorMLP.evaluate(prediction_lr)

print("Accuracy = %g" % accuracy_lr)
print("Test Error = %g" % (1.0 - accuracy_lr))

# COMMAND ----------

evaluatorLR = BinaryClassificationEvaluator(labelCol="agree_level", rawPredictionCol="prediction", metricName="areaUnderROC")
roc_result_lr = evaluatorLR.evaluate(prediction_lr)
print("Test ROC = %g" % roc_result_lr)

# COMMAND ----------

dt = DecisionTreeClassifier(labelCol="agree_level", featuresCol="features_norm", maxDepth=7,  weightCol="weight")
pipeline_dt = Pipeline(stages=[stringIndexer_attitude, regex_tokenizer, remover, cv, normalizer, dt, labelConverter])

# Fit the pipeline to training documents. 
model_dt = pipeline_dt.fit(train_data)

# COMMAND ----------

model_dt.save("/FileStore/data/dt")

# COMMAND ----------

prediction_dt = model_dt.transform(test_data)
evaluatorDT = MulticlassClassificationEvaluator(labelCol="agree_level", predictionCol="prediction", metricName="accuracy")
accuracy_dt = evaluatorDT.evaluate(prediction_dt)

print("Accuracy = %g" % accuracy_dt)
print("Test Error = %g" % (1.0 - accuracy_dt))

# COMMAND ----------

evaluatorDT = BinaryClassificationEvaluator(labelCol="agree_level", rawPredictionCol="prediction", metricName="areaUnderROC")
roc_result_dt = evaluatorDT.evaluate(prediction_dt)
print("Test ROC = %g" % roc_result_dt)

# COMMAND ----------

data = {'Model':  ['Logistic Regression', 'Random Forest', 'Random Forest2', 'Decision Tree'],
        'Hyperparameter': ['maxIter=100', 'numTrees=60, maxDepth=7, weight adjusted', 'numTrees=30, maxDepth=7, weight adjusted', 'maxDepth=7, weight adjusted'],
        'Accuracy': [accuracy_lr, accuracy_rf, accuracy_rf2, accuracy_dt],
        'ROC': [roc_result_lr, roc_result_rf, roc_result_rf2, roc_result_dt]
        }

df = pd.DataFrame(data)
df

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Time Series Model

# COMMAND ----------

# MAGIC %md
# MAGIC In this part, the team shall approach the time series structure of youtube and Twitch subreddit's hourly submission activity with SARIMA modeling techniques.

# COMMAND ----------

plt.figure(figsize=(16, 8))
sns.set_theme(style='darkgrid')  # 'whitegrid', 'dark', 'white', 'ticks'
ax = sns.lineplot(x='created_utc',
                  y='count',
                  hue='subreddit',
                  data=number_sub_byhour).set(title='Number of Submissions Per Hour')
plt.tight_layout()
plt.show()

# COMMAND ----------

ts_ytb = number_sub_byhour[number_sub_byhour['subreddit'] == 'youtube']
ts_twc = number_sub_byhour[number_sub_byhour['subreddit'] == 'Twitch']

# COMMAND ----------

ts_twc.head()

# COMMAND ----------

ts_ytb = ts_ytb.drop(columns = 'subreddit').set_index('created_utc').sort_index()
ts_twc = ts_twc.drop(columns = 'subreddit').set_index('created_utc').sort_index()

# COMMAND ----------

ts_ytb.head()

# COMMAND ----------

import matplotlib.dates as mdates
ax1 = ts_ytb.plot(figsize=(12, 8))
ax1.xaxis.set_major_locator(mdates.MonthLocator())
plt.tight_layout()
plt.show()

# COMMAND ----------

number_sub_byhour

# COMMAND ----------

plt.figure(figsize=(16, 8))
sns.set_theme(style='darkgrid')  # 'whitegrid', 'dark', 'white', 'ticks'
plt.plot(ts_ytb).set(title='Number of Submissions Per Hour')
#sns.lineplot(data=ts_twc)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Here, a previously written time series analysis toolbox will be called for several helper functions and tools.

# COMMAND ----------

import requests
my = requests.get('https://raw.githubusercontent.com/herrzilinski/toolbox/main/handy.py')
open('handy.py', 'w').write(my.text)

# COMMAND ----------

sc = spark.sparkContext
sc.addPyFile('handy.py')
from handy import ADF_Cal, kpss_test, ACF_PACF_Plot, differencing, GPAC_cal, GPAC_cal, SARIMA_Estimate

# COMMAND ----------

ADF_Cal(ts_ytb)

# COMMAND ----------

kpss_test(ts_ytb)

# COMMAND ----------

ADF_Cal(ts_twc)

# COMMAND ----------

kpss_test(ts_twc)

# COMMAND ----------

# MAGIC %md
# MAGIC Based on ADF and KPSS test results, both Twitch and Youtube subreddit's hourly submission are stationary.

# COMMAND ----------

ACF_PACF_Plot(ts_ytb, lags=50, series_name='Hourly Submission of Youtube Subreddit')

# COMMAND ----------

ACF_PACF_Plot(ts_twc, lags=50, series_name='Hourly Submission of Twitch Subreddit')

# COMMAND ----------

# MAGIC %md
# MAGIC The ACF/PACF plot suggest the time series are underdifferenced.

# COMMAND ----------

y_H1 = differencing(ts_ytb.to_numpy(), order=1)

# COMMAND ----------

ACF_PACF_Plot(y_H1, lags=50, series_name='Hourly Differencing')

# COMMAND ----------

GPAC_table = GPAC_cal(y_H1, 25, 12, 12, series_name='Differenced Series', astable=True)
fig, ax = plt.subplots()
sns.heatmap(GPAC_table, annot=True, vmin=-1, vmax=1, cmap='RdBu', ax=ax)

fig.suptitle(f'GPAC Table of Differenced Series')
fig.tight_layout()
fig.set_size_inches(16, 7)
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Combining the information from ACF/PACF and GPAC, we are looking for a ARIMA(0, 1, 1) or ARIMA(1, 1, 1) model for the differenced time series.

# COMMAND ----------

import statsmodels.api as sm
model_MA1 = sm.tsa.SARIMAX(ts_ytb, order=(0, 1, 1)).fit(trend='nc', disp=0)
model_MA1.summary()

# COMMAND ----------

# MAGIC %md
# MAGIC the MA factor in the time series is potent. Now we will compare it with ARIMA(1,1,1) model.

# COMMAND ----------

model_AR1MA1 = model = sm.tsa.SARIMAX(ts_ytb, order=(1, 1, 1)).fit(trend='nc', disp=0)
model_AR1MA1.summary()

# COMMAND ----------

# MAGIC %md
# MAGIC Comparing with the previous model, ARIMA(1,1,1) model has improved AIC and BIC. AR argument added is significant as well, judjing by the coefficient's confidence interval.

# COMMAND ----------

model_AR2MA1 = model = sm.tsa.SARIMAX(ts_ytb, order=(2, 1, 1)).fit(trend='nc', disp=0)
model_AR2MA1.summary()

# COMMAND ----------

# MAGIC %md
# MAGIC By adding one more AR argument, the confidence interval for its coefficient is much closer to zero. The BIC improvement from adding it is also minimal. We should use the ARIMA(1,1,1) model for describing youtube subreddit's hourly submission activity.

# COMMAND ----------

t_H1 = differencing(ts_twc.to_numpy(), order=1)

# COMMAND ----------

ACF_PACF_Plot(t_H1, lags=50, series_name='Hourly Differencing')

# COMMAND ----------

GPAC_table = GPAC_cal(t_H1, 25, 12, 12, series_name='Differenced Series', astable=True)
fig, ax = plt.subplots()
sns.heatmap(GPAC_table, annot=True, vmin=-1, vmax=1, cmap='RdBu', ax=ax)

fig.suptitle(f'GPAC Table of Differenced Series')
fig.tight_layout()
fig.set_size_inches(16, 7)
fig.show() 

# COMMAND ----------

# MAGIC %md
# MAGIC By reading the GPAC and ACF/PACF plot, we can see the structure of Twitch subreddit's submission activity is similar to that of youtube, it follows a MA1 structure as well.

# COMMAND ----------

model_MA1_t = model = sm.tsa.SARIMAX(ts_twc, order=(0, 1, 1)).fit(trend='nc', disp=0)
model_MA1_t.summary()

# COMMAND ----------

model_AR1MA1_t = model = sm.tsa.SARIMAX(ts_twc, order=(1, 1, 1)).fit(trend='nc', disp=0)
model_AR1MA1_t.summary()

# COMMAND ----------

# MAGIC %md
# MAGIC Adding AR element is redundant to the model. ARIMA(0,1,1) is enough for this time series.

# COMMAND ----------


