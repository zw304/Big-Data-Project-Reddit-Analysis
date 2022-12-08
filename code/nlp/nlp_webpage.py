# Databricks notebook source
# MAGIC %md
# MAGIC # Exploring reddit data using Spark: NLP Tasks

# COMMAND ----------

# MAGIC %md
# MAGIC ## Introduction
# MAGIC YouTube and Twitch: The online video heavyweight battle of the century.
# MAGIC 
# MAGIC While there are many similarities between Twitch and YouTube - for example, they are both audio/video sharing platforms - there are also many differences between the two platforms.
# MAGIC 
# MAGIC The most significant difference between Twitch and YouTube is the viewer's interest. Twitch caters to live-stream hosts and is the largest live-streaming platform in the world. On the other hand, YouTube uses an excellent search engine to help those interested in a specific topic find videos that match their heart's expectations.
# MAGIC 
# MAGIC While the two video sites have different focuses, they also have similar modules. YouTube, for example, has a live streaming system, while Twitch is also drawing on more different categories of video submissions.
# MAGIC 
# MAGIC This study examines the difference in users' focus using these two video platforms by comparing two different subreddits on Reddit, 'youtube' and 'Twitch'. The dataset for this notebook is described in [The Pushshift Reddit Dataset](https://arxiv.org/pdf/2001.08435.pdf) paper.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Business Questions Addressed In This Report:
# MAGIC ### 1. What’s the Time series patterns of submissions number?
# MAGIC #### Business Goal: 
# MAGIC Visualize time series patterns for Twitch comments and Youtube comments. 
# MAGIC ### 2. Sentiments analysis for Youtube and Twitch Reddits
# MAGIC #### Business Goal: 
# MAGIC Analyze redditers’ sentiments score and make comparison between these two video based platforms.
# MAGIC ### 3. Provide a list of technical problems, what is the frequency that users have on these technical problems of each platform? / Any suggestions for twitch and youtube to improve based on users’ post? 
# MAGIC #### Business Goal: 
# MAGIC The goal of this business question is to help twitch and youtube discover the most frequent problems that users meet, and help them to improve the user experience. The programming language used for this question would be Python and Spark. 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Executive Summary

# COMMAND ----------

# MAGIC %md
# MAGIC This report aims to study the statistical and semantic properties of Youtube and Twitch subreddit in Pushshifr Reddit dataset, to answer several proposed analytical questions. By comparing the daily number of submission activities with the stock prices of respective parent companies of the two platforms, the research team concludes that spikes in submission activities may be linked to daily closing price of parent companies, yet the overall correltaion is not significant. The research team used stop words removal, stemming/lemmatizing, and normalization pipelines from John Snow LABS to engineer the dataset foo further analysis. Word/token importance is established by using word count and TFIDF method. Word counts shows users on both subreddit tends to mention technical keywords to promote specific channel or account. TFIDF reveals slightly more insight, as Youtube subredditers' word choices are more adult-orientated, while Twitch users focus more on contents and types of streaming. Several keywords related to technical difficulties and user experience were studied. It shows that advertisment and sponsorship related issues were complained the most in both platforms, while Youtube subredditers suffered significantly more. The pattern shows the "Freemium" monetization scheme is gravely impacting viewers' experience.
# MAGIC 
# MAGIC Vivekn pre-trained model was used to infer user sentiment. Twitch and YouTube comments’ score distribution were respectively visualized under three situations: “score below zero”, “score equal to zero” and score over zero”. It’s easy to find that no matter for Twitch or YouTube comments, most of their score is over zero, than equal to zero or below zero. Which means that when giving votes to posts, people are more likely to have a mutual agreement either on negatively or positively. It shows that people in YouTube and twitch have their preferences on topics and posts. When the comments’ are deemed positive, their scores are more likely to be above zero(received more upvotes than downvotes) on average, and vise versa. This suggests both Twitch and YouTube subreddit have maintained a relatively healthy and constructive environments for interaction, a job well done by the communities and moderators. Sentiment analysis about the submission content also provides an insight that the users tend to have a more negative opinion and would get a more passive agreement on posts in Youtube. For Twitch, users’ experience and attitude towards the posts are more gentle. Suggestions can be given to the Youtube that operators should put more emphasis on users’ experience.

# COMMAND ----------

comments_error_df2 = comments_error_df[comments_error_df['error_dummy']!="normal"]
plt.figure(figsize=(12, 7))
sns.set(font_scale=2)
sns.barplot(data=comments_error_df2, x="error_dummy", y="count", hue="subreddit").set(title='Technical Problems of Each Platform', xlabel='Error Type')
plt.xticks(rotation=45)
plt.show()

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

sns.set(font_scale=1.2)
ax = df.plot.bar(title=" Comments' Score Distribution for Youtube and Twitch") 
ax.set_xlabel("Score in different situations")
plt.xticks(rotation=30)
ax.set_ylabel("Comments Number")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analysis Report

# COMMAND ----------

# MAGIC %md
# MAGIC ### User Activities And Parent Company Stock Price Comparison

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
# MAGIC After coverting the 'created_utc' column to datetime, the team plotted the number of submissions for each subreddit per day. It can seen that the daily activities for both subreddits are declining over the past two years. For most of times YouTube subreddit have more daily submissions than Twitch subreddit, while occasional spikes for YouTube are more significant, usually indicating a service interruption (which will be discussed later). The stock price time plot for the parent company of both platforms are overlayed for reference. It shows that the overall trend of the submission activities in these two subreddits does not reflect the stock price of the two companies, certain “pikes” in the capital market does correlates with more discussions on reddit.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Words of Importance

# COMMAND ----------

# MAGIC %md
# MAGIC The team studied the importance of words using word count and TFIDF for both subreddits’ submission. Below three figures shows the most commmon words for submissions and comments. For the most common words apart from the two subreddit names themselves, Youtube subreddit have “video”, “channel”, “comment” take the top 3, while Twitch subreddit have “stream”, “streamer”, “chat” on the top hits. For authors comments, "post", "question" and "channel" are the top 3 topics for youtube subreddit, and for Twitch subreddit, "stream" and "rule" are mentioned mostly. This is clearly determined by the self-promotion efforts on both platforms, when the uploader/streamer asks viewers to interact and respond to their content to boost popularity.

# COMMAND ----------

mcw_df_title = result_mcw.limit(20).toPandas()
plt.figure(figsize=(16, 8))
sns.set(font_scale=1.5)
sns.barplot(data=mcw_df_title, x="word", y="count", hue="subreddit").set(title='Most Common Words for Submission Titles')
plt.xticks(rotation=45)
plt.show()

# COMMAND ----------

mcw_df_text = result_text_mcw.limit(20).toPandas()
plt.figure(figsize=(16, 8))
sns.set(font_scale=1.5)
sns.barplot(data=mcw_df_text, x="word", y="count", hue="subreddit").set(title='Most Common Words for Submission Bodies')
plt.xticks(rotation=45)
plt.show()

# COMMAND ----------

mcw_df_body = result_comments_mcw.limit(20).toPandas()
plt.figure(figsize=(16, 8))
sns.set(font_scale=1.3)
sns.barplot(data=mcw_df_body, x="word", y="count", hue="subreddit").set(title='Most Common Words for Comments')
plt.xticks(rotation=45)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC From plots below it could be inferred that YouTube subreddit have relatively more consistent score over length of post: Longer(or shorter) posts do not impact on the average score as much as Twitch subreddit. While Twitch sub-redditers tend to engage more actively for submissions from 120 to 140 characters, which is a normal length limit for social network platforms.

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
# MAGIC By extracting TFIDF score from Sparknlp’s pipeline, the research team has a better understanding to words used in submission titles. The F-word rules supreme in the Youtube subreddit. Adult contents are more prevalent in this subreddit compared to Twitch. Keywords include "share", "youd", "meditation", "beat" clearly shows user preference and attention. There is no surprise that the F-word rules supreme in the Youtube subreddit. Adult contents and offensive language are more prevalent in this subreddit. Twitch subreddit users clearly pays more attention to technical contents and interaction with other users. Several trending twitch channel made it in the top of this list. BM25 metric used by Elastic Search could do better job compared to TFIDF. This metric could be calculated for each token and compare results with TFIDF in the future.

# COMMAND ----------

pd.concat([tfidf_youtube, tfidf_twitch], axis=1)

# COMMAND ----------

tfidf_all_plot = pd.concat([tfidf_youtube, tfidf_twitch], axis=0)
plt.figure(figsize=(16, 8))
sns.set(font_scale=1.3)
sns.barplot(data=tfidf_all_plot, x="words", y="score", hue="subreddit").set(title='TFIDF Score for Submission Title')
plt.xticks(rotation=45)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Technical Problems of Each Platform

# COMMAND ----------

# MAGIC %md
# MAGIC The research team attempts to identify key areas where redditers complain the most in respective subreddits. The keywords searching can broadly classify common complaint into the following five categories: advertising or promotions, audio or sound related issues, image/video quality, connection speed, interface crash or corruption, and privacy issues by cookie collection. Ads related post by far leads other technical complaints by a significant margin, for both platforms. This is obviously dictated by the “Fremium” business pattern adopted by both companies. Youtube subredditers mention ads far more than Twitch suggest Youtube’s monetization channel is overly reliant on advertisement and sponsorship, to the extent that viewing experience for free users is greatly impacted. Next in line, audio quality and connection speed caught the attention of both platform users. It is worth noting that apart from ads and cookie related issues, Twitch redditers mention more of these keywords in almost every aspect. While this could be explained by higher users engagement and interaction for this subreddit, the management and technical team of Twitch should pay attention to their user experience regardless.

# COMMAND ----------

comments_error_df2 = comments_error_df[comments_error_df['error_dummy']!="normal"]
plt.figure(figsize=(16, 8))
sns.set(font_scale=2)
sns.barplot(data=comments_error_df2, x="error_dummy", y="count", hue="subreddit").set(title='Technical Problems of Each Platform', xlabel='Error Type')
plt.xticks(rotation=45)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sentiment Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC #### Sentiment Analysis on "Submissions" Data

# COMMAND ----------

# MAGIC %md
# MAGIC To provide business insight of users’ opinion for both Youtube and Twitch Reddits, this analysis process split the user into proportions toward topics and comments that were posted and replied. For example, to discover the relationship between the title of a post and the post contents in these two platforms, the following four plots are provided. The bar plots in pink are built based on if the title is posted as a question format while the blue bar plots focus on if the post gets a general agreement. 
# MAGIC 
# MAGIC To solve the business goal of what’s the relationship between users’ opinion and the post content, the following tables and plots are given after using the NLP model. The Score_dummy_final_sentiment_submissions is 0 when the number of upvotes and downvotes is the same. The upper table is for youtube and the lower one is for Twitch. Comparing these two plots, the numbers show that for both platforms, the content of posts that achieve a majority group of users’  agreement or disagreement is around 5 times higher than fair posts in youtube and 2 times higher than the one in Twitch. Also, among these posts, the number of users who have a negative opinion 1.08 higher than positive opinion users. However, users’ opinions on the Twitch platform are more balanced. This provides an insight that the users tend to have a more negative opinion and would get a more passive agreement on posts in Youtube. Though the volume of users for youtube and twitch is not the same, the following bar plots still show patterns about the agreement distributions. Combining with the table that shown above, suggestions can be given to the Youtube operators to put more emphasis on users’ experience. Based on the most common words generated by the NLP model, the operators can make decisions with more specific instructions. To achieve this goal better, a future NLP analysis would be given about the most frequent or popular words and topics that users interested in.

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

# MAGIC %md
# MAGIC 
# MAGIC The following tables and histograms are generated to discover the distribution of users’ attitude based on different title formats. The upper table is for youtube and the lower one is for twitch. They clearly show that for both youtube and twitch, the total distribution of titles without a question mark is higher than the one which follows a question format. For youtube, the users have more negative and passive opinions regardless if the title is posted as a question format. However, for Twitch, with the dummy variable of title equals to 1, users then have a more positive opinion which would bring the platform with a better impression. This also shows that the general opinion and sentiment of users in twitch is better than youtube.  

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
plt.title("The relationship between the title of a post and the post content(negative)")
plt.xlabel("If the title has a question format") 
plt.ylabel("Total Counts")

plt.figure(figsize=(10, 8))
sentiment_youtube_table2.plot.bar(x='title_dummy_final_sentiment_submissions', y='WrappedArray(positive)' ,color="blue" )
plt.title("The relationship between the title of a post and the post content(positive)")
plt.xlabel("If the title has a question format") 
plt.ylabel("Total Counts")
plt.show() 

## twitch 

plt.figure(figsize=(10, 8))
sentiment_twitch_table2.plot.bar(x='title_dummy_final_sentiment_submissions', y='WrappedArray(negative)',color="pink" )
plt.title("The relationship between the title of a post and the post content(negative)")
plt.xlabel("If the title has a question format") 
plt.ylabel("Total Counts")


plt.figure(figsize=(10, 8))
sentiment_twitch_table2.plot.bar(x='title_dummy_final_sentiment_submissions', y='WrappedArray(positive)' ,color="pink" )
plt.title("The relationship between the title of a post and the post content(positive)")
plt.xlabel("If the title has a question format") 
plt.ylabel("Total Counts")
plt.show() 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Sentiment Analysis on "Comments" Dataset 

# COMMAND ----------

# MAGIC %md
# MAGIC To better understand the comments data from Twitch and YouTube, Vivekn pre-training Model was used again in the “comments_nano” dataset. Considering the bar plot in the "Executive Summary" revealed data asymmetrical, pie plots were used here to help dive deeper on more specific distributions in each site under each situation. After overall analysis, when the comments’ score below zero(received more downvote than upvote), sentiment analysis results are more likely to be negative; And on the contrary, when the comments’ score over zero(received more upvote than downvote), the sentiments analysis results are more likely to be positive. This result caters to the normal expectations.

# COMMAND ----------

data = {'below_positive':8749,
        'below_negative':8004,
        'below_na':2787,
        'below_blank':523,}
colors = ['orange', 'green', 'cyan', 'skyblue']
explode = (0.1, 0.1, 0.1, 0.1) 
plt.pie(list(data.values()), explode=explode, labels=data.keys(), colors=colors, autopct='%2.1f%%', shadow=True, startangle=90)
plt.title("Twitch Comments Distribution Where Score Below 0")
plt.show()

data = {'below_positive':4025,
        'below_negative':3263,
        'below_na':1545,
        'below_blank':307,}
colors = ['orange', 'green', 'cyan', 'skyblue']
explode = (0.1, 0.1, 0.1, 0.1) 
plt.pie(list(data.values()), explode=explode, labels=data.keys(), colors=colors, autopct='%2.1f%%', shadow=True, startangle=90)
plt.title("Youtube Comments Distribution Where Score Below 0")
plt.show()


# COMMAND ----------

import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

data = {'equal_positive':10893,
        'equal_negative':9105,
        'equal_na':3393,
        'equal_blank':751,}
colors = ['orange', 'green', 'cyan', 'skyblue']
explode = (0.1, 0.1, 0.1, 0.1) 
plt.pie(list(data.values()), explode=explode, labels=data.keys(), colors=colors, autopct='%2.1f%%', shadow=True, startangle=90)
plt.title("Twitch Comments Distribution Where Score Equal to 0")
plt.show()

data = {'equal_positive':8167,
        'equal_negative':5919,
        'euqal_na':2992,
        'equal_blank':794,}
colors = ['orange', 'green', 'cyan', 'skyblue']
explode = (0.1, 0.1, 0.1, 0.1) 
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
plt.pie(list(data.values()), explode=explode, labels=data.keys(), colors=colors, autopct='%2.1f%%', shadow=True, startangle=90)
plt.title("Twitch Comments Distribution Where Score Over 0")
#plt.figure(figsize=(16, 8))
plt.show()

data = {'over_positive':194019,
        'over_negative':125797,
        'over_na':47488,
        'over_blank':10408,}
colors = ['orange', 'green', 'cyan', 'skyblue']
explode = (0.1, 0.1, 0.1, 0.1) 
plt.pie(list(data.values()), explode=explode, labels=data.keys(), colors=colors, autopct='%2.1f%%', shadow=True, startangle=90)
plt.title("Youtube Comments Distribution Where Score Over 0")
plt.show()
