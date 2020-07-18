# In[1]:


import re
import findspark
findspark.init()
import sys
import pyspark.sql.functions as func
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql import Row
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, StringIndexer
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import CountVectorizer
import warnings


# In[3]:


#Create spark session and SQLContext
#conf = SparkConf().setMaster("local[2]").setAppName("Sentiment Analysis")
sc = SparkSession.builder.config('spark.driver.maxResultSize', '10G').master("local[*]").getOrCreate()
sqc = SQLContext(sc)
#spark = SparkSession(sc)

# In[5]:
#sc.s

#Load archived sentiment training set
sent_csv = sqc.read.format('com.databricks.spark.csv').options(header='true', inferschema='true', quote='\"', escape='\"', delimiter=',', multiLine='true').load("archivednewssentiment.csv").repartition(100)
sent_csv = sent_csv.withColumn("Rounded", func.round(sent_csv["Polarity"], 1))
#Drop URL, Type, and Data
sent_csv = sent_csv.drop('URL', 'Type', 'Data', 'Subjectivity', 'Polarity')
sent_csv = sent_csv.dropna()

#sent_csv.show()
print(sent_csv.show(100))


# In[6]:


#Create traning, validation, and test sets
(train, val, test) = sent_csv.randomSplit([0.98, 0.1, 0.1], seed=1992)


# In[9]:


#Tokenize News string into Words
token = Tokenizer(inputCol="News", outputCol="Words")

#Hash words into signatures"
cv = CountVectorizer(vocabSize=2**16, inputCol="Words", outputCol='Sig')

#Find Inverse Document Frequency, minDocFreq should be tweaked
idf = IDF(inputCol="Sig", outputCol="features", minDocFreq=5)

#Create label index set from Polarity column
labels = StringIndexer(inputCol = "Rounded", outputCol = "label")

log_reg = LogisticRegression(maxIter=5)
#Create pipeline for dataset processing
pipeline = Pipeline(stages=[token, cv, idf, labels, log_reg])

#Fit pipeline
pipe_fit = pipeline.fit(train)
print("Saving Model")

#Save Pipeline
pipe_fit.write().overwrite().save("SentimentModel")

sys.exit(23)


# In[ ]:




