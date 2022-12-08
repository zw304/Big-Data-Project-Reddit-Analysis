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
# MAGIC ## Read the data
# MAGIC Let us begin with listing the file in the bucket. There are two folders `comments` and `submissions`, we will read both of them into separate Spark dataframes. The raw data is in [parquet format](https://www.databricks.com/glossary/what-is-parquet#:~:text=What%20is%20Parquet%3F,handle%20complex%20data%20in%20bulk.).

# COMMAND ----------

dbutils.fs.ls("abfss://anly502@marckvaismanblob.dfs.core.windows.net/reddit/parquet") 

# COMMAND ----------

comments = spark.read.parquet("abfss://anly502@marckvaismanblob.dfs.core.windows.net/reddit/parquet/comments")
#submissions = spark.read.parquet("abfss://anly502@marckvaismanblob.dfs.core.windows.net/reddit/parquet/submissions")

# COMMAND ----------

# MAGIC %md
# MAGIC The `count` operation below will take several minutes, it is show here because it provides an oppurtunity to look at the Spark UI will a computation is in progress. Expand the `Spark Jobs` output in the result cell to see how the job is progressing and then drill down into task details.

# COMMAND ----------

# comments_row_count = comments.count()
# comment_col_count = len(comments.columns)

# COMMAND ----------

# print(f"shape of the comments dataframe is {comments_row_count:,}x{comment_col_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC A glimpse of the data and the schema.

# COMMAND ----------

# comments.show()

# COMMAND ----------

# comments.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exploratory Data Analysis
# MAGIC 
# MAGIC Here is an example of some EDA. But, before we do that let us setup some local directory structure so that that the results can be checked in into the repo.

# COMMAND ----------

## create a directory called data/plots and data/csv to save generated data
import os
# PLOT_DIR = os.path.join("data", "plots")
# CSV_DIR = os.path.join("data", "csv")
# os.makedirs(PLOT_DIR, exist_ok=True)
# os.makedirs(CSV_DIR, exist_ok=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### How many subreddits are there and which are the more popular ones?
# MAGIC 
# MAGIC One of the first questions we can ask is how many subreddits are there and which ones are the top 10 based on the number of submissions.

# COMMAND ----------

# from pyspark.sql.functions import col, asc,desc
# submissions_by_subreddit = submissions.groupBy("subreddit").count().orderBy(col("count"), ascending=False).collect()

# COMMAND ----------

# len(submissions_by_subreddit)

# COMMAND ----------

# submissions_youtube = submissions.filter(submissions.subreddit == "youtube")

# COMMAND ----------

# submissions_twitch = submissions.filter(submissions.subreddit == "Twitch")

# COMMAND ----------

# submissions_youtube.select('author').show(5)

# COMMAND ----------

# submissions_youtube.printSchema()

# COMMAND ----------

comments_youtube = comments.filter(comments.subreddit == "youtube")

# COMMAND ----------

comments_twitch = comments.filter(comments.subreddit == "Twitch")

# COMMAND ----------

# submissions_twitch.columns

# COMMAND ----------

# submissions_twitch.count() 

# COMMAND ----------

# this code saves the data as a csv just for illustration, use parquet for faster analytics operations.
# save to DBFS without size limitation.
# submissions_youtube.select("id", "author_id", "title", "selftext", "num_comments", "over_18", "score").write.format('com.databricks.spark.csv').save("/FileStore/data/sub_youtube")

# COMMAND ----------

# dbutils.fs.rm("/Workspace/Repos/ql150@georgetown.edu/fall-2022-project-eda-adb-project-group-26/data/csv/subreddits_count_by_submission.csv", True)

# COMMAND ----------

# submissions = submissions_youtube.union(submissions_twitch)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Saving data to DBFS
# MAGIC Sometimes we may want to save intermediate data, especially if it is big and took a significant time to generate, in DBFS. The following code shows an example of this. We save submissions on the `youtube` and `twitch` subreddit into dbfs so that we can read from it at a later stage.

# COMMAND ----------

# submissions.select("id", "author_id", "subreddit", "title", "selftext", "num_crossposts", "num_comments", "is_self", "is_video", "is_crosspostable", "over_18", "promoted", "stickied", "score", "created_utc").write.parquet("/FileStore/data/submissions_all.parquet") 

# COMMAND ----------

# submissions_youtube.select("id", "author_id", "subreddit", "title", "selftext", "num_crossposts", "num_comments", "is_self", "is_video", "is_crosspostable", "over_18", "promoted", "stickied", "score", "created_utc").write.parquet("/FileStore/data/submissions_youtube.parquet") 

# COMMAND ----------

# submissions_twitch.select("id", "author_id", "subreddit", "title", "selftext", "num_crossposts", "num_comments", "is_self", "is_video", "is_crosspostable", "over_18", "promoted", "stickied", "score", "created_utc").write.parquet("/FileStore/data/submissions_twitch.parquet") 

# COMMAND ----------

 comments_youtube.select("id", "link_id", "author", "body", "score", "created_utc", "subreddit").write.parquet("/FileStore/data/comments_youtube.parquet") 

# COMMAND ----------

 comments_twitch.select("id", "link_id", "author", "body", "score", "created_utc", "subreddit").write.parquet("/FileStore/data/comments_twitch.parquet") 

# COMMAND ----------

comments = comments_youtube.union(comments_twitch)

# COMMAND ----------

# submissions_youtube = spark.read.csv("/FileStore/data/sub_youtube")
# submissions_youtube.show()

# COMMAND ----------

comments.select("id", "link_id", "author", "body", "score", "created_utc", "subreddit").write.parquet("/FileStore/data/comments_all.parquet") 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Data From DBFS

# COMMAND ----------

submissions_youtube=spark.read.parquet("/FileStore/data/submissions_youtube.parquet")
submissions_twitch=spark.read.parquet("/FileStore/data/submissions_twitch.parquet")

# COMMAND ----------

submissions = submissions_youtube.union(submissions_twitch)

# COMMAND ----------

comments_youtube=spark.read.parquet("/FileStore/data/comments_youtube.parquet")
comments_twitch=spark.read.parquet("/FileStore/data/comments_twitch.parquet")

# COMMAND ----------

comments = comments_youtube.union(comments_twitch)

# COMMAND ----------

type(submissions_youtube)

# COMMAND ----------

display(submissions_youtube)

# COMMAND ----------

import pyspark.sql.functions as f
submissions_youtube = submissions_youtube.withColumn('created_utc', f.from_unixtime("created_utc", "MM/dd/yyyy HH:mm:ss"))
submissions_twitch = submissions_twitch.withColumn('created_utc', f.from_unixtime("created_utc", "MM/dd/yyyy HH:mm:ss"))
submissions = submissions.withColumn('created_utc', f.from_unixtime("created_utc", "MM/dd/yyyy HH:mm:ss"))

# COMMAND ----------

import pyspark.sql.functions as f
df_t = submissions.withColumn('created_utc', f.from_unixtime("created_utc", "MM/dd/yyyy HH"))

# COMMAND ----------

number_sub_byhour = df_t.groupby("created_utc", "subreddit").count().toPandas()

# COMMAND ----------

number_sub_byhour["created_utc"] = pd.to_datetime(number_sub_byhour["created_utc"])

# COMMAND ----------

number_sub_byhour.head()

# COMMAND ----------

len(pd.unique(number_sub_byhour["created_utc"]))

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

number_sub_byhour.to_csv("/dbfs/FileStore/data/number_sub_byhour.csv", index=False)

# COMMAND ----------

submissions_youtube.show()

# COMMAND ----------

comments = comments.withColumn('created_utc', f.from_unixtime("created_utc", "MM/dd/yyyy"))

# COMMAND ----------

comments.show()

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

number_sub = submissions.groupby('subreddit').count()
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
sns.set(font_scale=1.5)
plt.xticks(rotation=0)
# plt.tight_layout()
plt.show()

# COMMAND ----------

df_numbercom = comments.groupby('subreddit').count().cache().toPandas()
df_numbercom

# COMMAND ----------

plt.figure(figsize=(10, 8))
sns.set(font_scale=1.5)
df_numbercom.plot.bar(x='subreddit', y='count') 
plt.title("The popularity of Comments by subreddits")
plt.xticks(rotation=0)
plt.ylabel("Number of Comments") 
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC The bar plot shows the polulation of submissions in these two different subreddit. It is obvious that youtube had more submission then Twitch from January 1, 2021 to Auguest 21, 2022.

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA2: Trend of Submission Activities

# COMMAND ----------

number_sub_byday = submissions.groupby("created_utc", "subreddit").count().toPandas()

# COMMAND ----------

number_sub_byday["created_utc"] = pd.to_datetime(number_sub_byday["created_utc"])

# COMMAND ----------

# MAGIC %md
# MAGIC Below line plot shows the number of submission per day between January 1, 2021 to Auguest 31, 2022. 

# COMMAND ----------

plt.figure(figsize=(16, 8))
sns.set_theme(style='darkgrid')  # 'whitegrid', 'dark', 'white', 'ticks'
ax = sns.lineplot(x='created_utc',
                  y='count',
                  hue='subreddit',
                  data=number_sub_byday).set(title='Number of Submissions Per Day')
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC After coverting the 'created_utc' column to datetime, we plotted the number of submissions for each subreddit per day. We can see that overall the daily activities for both subreddits are declining over the past two years. For most of times YouTube subreddit have more daily submissions then Twitch subreddit, while occasional spikes for YouTube are more significant, usually indicating a service interruption (which will be discussed later). The stock price time plot for the parent company of both platforms are included in the following chunks for reference.

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA3: Stock Price by Day(External Dataset)

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

stock.head()

# COMMAND ----------

fig, axes = plt.subplots(figsize=(16,8))
sns.set_theme(style='darkgrid')  # 'whitegrid', 'dark', 'white', 'ticks'
ax1 = sns.lineplot(x = 'Date',
                   y = 'GOOG',
                   label = 'GOOG',
                   data = stock)
ax2 = sns.lineplot(x = 'Date',
                   y = 'AMZN',
                   label = 'AMZN',
                   data = stock).set(title='Daily Closing Stock Price of Parent Comapines')

plt.xticks(rotation = 45)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA4: Comments features for submission in two subreddit

# COMMAND ----------

# MAGIC %md
# MAGIC The below table shows total number of comments for each subreddit. Even youtube has more submissions then Twitch, twitch has more comments, indicates that Twitch has higher user engagement. 

# COMMAND ----------

submissions.groupBy('subreddit').sum("num_comments").show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import sum,avg,max, min

# COMMAND ----------

submissions.groupBy('subreddit').agg(sum("num_comments").alias("sum_comments"), \
         avg("num_comments").alias("avg_comments"), \
         min("num_comments").alias("min_comments"), \
         max("num_comments").alias("max_comments")).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Above table shown more detail for the number of comments for submissions. the maximum number of comments for youtube is 11,033, which is close to 10 times to Twitch (1333). The average of number of comments under each submission shows that Twitch has twice average of comments than youtube. This recomfirmed the statement we made earlier: Twitch subreddit has higher user engagement than youtube subreddit.
# MAGIC 
# MAGIC Variable `score` was calculated by number of upvoting - number of downvoting. A surprising phenomenon is that the minimum score are both zero for these two subreddits. 

# COMMAND ----------

submissions.groupBy('subreddit').agg(sum("score").alias("sum_score"), \
         avg("score").alias("avg_score"), \
         min("score").alias("min_score"), \
         max("score").alias("max_score")).show(truncate=False) 

# COMMAND ----------

# MAGIC %md
# MAGIC The summary table for score(upvote - downvote) shows the similar pattern as number of comments.

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA5: Most popular topics in two subreddits

# COMMAND ----------

# MAGIC %md
# MAGIC ### Distribution of comments

# COMMAND ----------

bins, counts = submissions.select('num_comments').rdd.flatMap(lambda x: x).histogram(100)
plt.figure(figsize=(8,6))
plt.hist(bins[:-1], bins=bins, weights=counts)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Above plot gives the distribution for number of comments. As it shows the data is significant skewed. This makes sense, since most of the submissions do not have comments. In a later step, we will make this variable a dummy variable, so that we can see the characteristics of a submission without a comment and a submission with a comment.

# COMMAND ----------

bins, counts = submissions.select('score').rdd.flatMap(lambda x: x).histogram(100)
plt.figure(figsize=(8,6))
plt.hist(bins[:-1], bins=bins, weights=counts)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Above plot gives the distribution for scores. As it shows the data is significant skewed. This makes sense, since most of the submissions has very few people to upvote or downvote. In a later step, we will make this variable into a dummy variable, so that we can see the characteristics of a submission with scores.

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Most comments topics

# COMMAND ----------

# MAGIC %md
# MAGIC From the above analysis, we can see that for youtube and Twtich, the submissions with the most comments have 11033 and 1333 coments respectively. We are curious about what kind of topic makes such amount of comments.
# MAGIC 
# MAGIC The most comments topic for youtube is "Youtube.com is down right now. Please keep all discussion of the issue to this megathread." This topic has so many comments because the failure problem is concentrated in one submission.
# MAGIC 
# MAGIC The most comments topic for Twitch is "Over 120GB of Twitch website data has been leaked online (source code, encrypted passwords, streamer payouts, etc.)". This means that Twitch's data leaking was a hot topic of discussion for two years.

# COMMAND ----------

submissions.select('subreddit', 'title', 'num_comments').where(submissions.subreddit=='youtube').sort(desc('num_comments')).show(10, truncate=False)

# COMMAND ----------

submissions.select('subreddit', 'title', 'num_comments').where(submissions.subreddit=='Twitch').sort(desc('num_comments')).show(10, truncate=False)

# COMMAND ----------

df_top5com = submissions.select('subreddit', 'title', 'num_comments').where((submissions.num_comments >= 2554) | ((submissions.num_comments == 1333) & (submissions.subreddit=='Twitch'))).sort(desc('subreddit'), desc('num_comments')).cache().toPandas()

# COMMAND ----------

display(df_top5com)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Highest score topics.

# COMMAND ----------

# MAGIC %md
# MAGIC From the above analysis, we can see that for youtube and Twtich, the submissions with the highest score of 22698 and 9714 respectively. We are curious about what kind of topic has so many people upvote it.
# MAGIC 
# MAGIC The highest score topic for youtube is "Youtube.com is down right now. Please keep all discussion of the issue to this megathread." As users use this submission to discuss and solve problems, it causes most people to upvote this submission.
# MAGIC 
# MAGIC The highest score topic for Twitch is "Twitch Discoverability In A Nutshell".

# COMMAND ----------

from pyspark.sql.functions import sum, col, desc
submissions.select('subreddit', 'title', 'score').where(submissions.subreddit=='youtube').sort(desc('score')).show(10, truncate=False)

# COMMAND ----------

submissions.select('subreddit', 'title', 'score').where(submissions.subreddit=='Twitch').sort(desc('score')).show(10, truncate=False)

# COMMAND ----------

df_top5 = submissions.select('subreddit', 'title', 'score').where(((submissions.score >= 14154) & (submissions.subreddit=='youtube')) | ((submissions.score >= 7719) & (submissions.subreddit=='Twitch'))).sort(desc('subreddit'), desc('score')).cache().toPandas()

# COMMAND ----------

display(df_top5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA6: Dummy Variable Distributions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dummy Score

# COMMAND ----------

#Count the score equal to "0"
submissions.filter(submissions.score == 0).count()

# COMMAND ----------

#Add new dummy variable "score_dummy":'0' represent score=0, '1' represent score!=0
from pyspark.sql.functions import when
submissions.withColumn('score_dummy', when(submissions.score ==0, 0).otherwise(1)).show(5)
submissions = submissions.withColumn('score_dummy', when(submissions.score ==0, 0).otherwise(1))

# COMMAND ----------

number_dummy_score = submissions.groupby('subreddit', 'score_dummy').count().cache()
number_dummy_score.toPandas()

# COMMAND ----------

sns.barplot(data=number_dummy_score.toPandas(), x="subreddit", y="count",hue="score_dummy")
plt.title('Dummy Scores distribution of youtube & Twitch')
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC This bar plot illustrates 'number_dummy_score' with subreddit as x axis and the total count for the score dummy varibles as y axis. This plot is generated for discoving the quality of post for social media platforms of Twitch and Youtube. From the plot, it is clearly showed that the most of the posts are in line with majority opinions. 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dummy Title

# COMMAND ----------

#Add new dummy variable "title_dummy":'0' represent the title is not a format of question =0, '1' represent the title format is a question

# just for test:
#submissions.filter(col("title").rlike("/?.*$")).show()

from pyspark.sql.functions import when
submissions.withColumn('title_dummy', when(col("title").rlike("\?"), 1).otherwise(0)).show(5)
submissions = submissions.withColumn('title_dummy', when(col("title").rlike("\?"), 1).otherwise(0))

# COMMAND ----------

number_title_dummy = submissions.groupby('subreddit', 'title_dummy').count().cache()
number_title_dummy.toPandas()

# COMMAND ----------

import matplotlib.pyplot as plt
data = {'Twich_title_dummy = 0 ':66750,
        'Twich_title_dummy = 1 ':30493,
        'Youtube_title_dummy = 0':46772,
        'Youtube_title_dummy = 1':30493,}
colors = ['orange', 'green', 'cyan', 'skyblue']
explode = (0.1, 0.1, 0.1, 0.1) 
plt.pie(list(data.values()), explode=explode, labels=data.keys(), colors=colors, autopct='%2.1f%%', shadow=True, startangle=90)
plt.axis('equal')
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC This pie chart pillustrates the distribution of 'number_title_dummy'. Four different colors stands for varibles of youtube and twitch with title dummy varibles of 0 or 1 where title_dummy = 1 means the title contains a question. This plot gives us insights that the percentage of post in both youtube and twitch are very similar(around 17%) when it is a question format. 

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA7: Proportion of topics that are Not-Safe-For-Work

# COMMAND ----------

number_0ver18 = submissions.groupby('subreddit', 'over_18').count().cache()
number_0ver18

# COMMAND ----------

number_0ver18.toPandas()

# COMMAND ----------

sns.barplot(data=number_0ver18.toPandas(), x="subreddit", y="count", hue="over_18")
plt.title('Over 18 Comments in Youtube and Twitch Data')
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Within these two subreddit, Twitch redditer are less inclined to discuss contents that are Not-Safe-For-Work.

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA8: Relationship between length of title and mean score

# COMMAND ----------

submissions = submissions.withColumn('len_title', f.length('title'))

# COMMAND ----------

score_lentitle = submissions.groupBy('len_title', 'subreddit').agg({"score": "mean"}).toPandas()

# COMMAND ----------

score_lentitle

# COMMAND ----------

plt.figure(figsize=(16, 8))
sns.set_theme(style='darkgrid')  # 'whitegrid', 'dark', 'white', 'ticks'
ax = sns.lineplot(data=score_lentitle, x="len_title", y="avg(score)", hue="subreddit")
plt.tight_layout()
plt.title('Correlation Between title length and average score in YouTube and Twitch Data')
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC From the plot we can see YouTube subreddit have relatively more consistent score over lenth of post: Longer(or shorter) post does not impact on the average score as much as Twitch subreddit. While Twitch subredditers tends to engage more actively for submissions from 120 to 140 characters, which is a normal length limit for social network platforms.
