# In[1]:


import re
import findspark
findspark.init()
import sys
import pyspark.sql.functions as func
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql import Row
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.classification import LogisticRegressionModel
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import CountVectorizer
import matplotlib.pyplot as plt
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
sent_csv = sent_csv.drop('URL', 'Type', 'Data', 'Subjectivity')
sent_csv = sent_csv.dropna()

#sent_csv.show()
print(sent_csv.count())


# In[6]:


#Create traning, validation, and test sets
(train, val, test) = sent_csv.randomSplit([0.98, 0.1, 0.1], seed= 1992)

#Fit pipeline
pipe_fit = PipelineModel.load("SentimentModel")

#Get predictions from validation set
pred = pipe_fit.transform(val)

#Compute accuracy
accuracy = pred.filter(pred.label == pred.prediction).count() / float(val.count())

#Use evaluator to find ROC AUC of model
ev = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction")
vval = ev.evaluate(pred)

#print ROC and AUC
print("Accuracy: {}".format(accuracy))
print("ROC AUC: {}".format(vval))

#Write values to text file
file = open("Results.txt", "w+")
W = [str(accuracy), str(vval)]
file.writelines(W)
file.close()



sys.exit(23)


# In[ ]:




