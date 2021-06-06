import os
import sys
import urllib.parse
from django.shortcuts import render, redirect
from django.views.generic.base import TemplateView
from .forms import PostForm
import boto3
import pandas
import io
import re
import time

db = 'result_table'
#Number of rows in 500MB file
#n_rows = 1714843
#Number of rows in 1GB file
#n_rows = 3491389
#Number of rows in dump.txt file
n_rows = 4715501

params = {
    'region': '[YOUR REGION]',
    'database': 'sampledb',
    'bucket': '[BUCKET TO SAVE RESULTS]',
    'path': 'Unsaved/2021/output',
    'query': 'SELECT * FROM ' + db + ' LIMIT 100'
}
# Create your views here.

def hi(request):
   return render(request, 's3_website/home.html')


def tab(request):
   return render(request, 's3_website/dataset_page.html')
# Create your views here.

def getDB(request):
   params['query'] = 'SELECT * FROM ' + db
   table = query()
   return render(request, 's3_website/home.html', {'table': table})

def getDB_2(request):
   params['query'] = 'SELECT * FROM ' + db + ' LIMIT 1000'
   table = query()
   return render(request, 's3_website/athena.html', {'table': table})

def athena(request):
   return render(request, 's3_website/athena.html')


def athena_query(client, params):
   response = client.start_query_execution(
      QueryString=params["query"],
      QueryExecutionContext={
         'Database': params['database']
      },
      ResultConfiguration={
         'OutputLocation': 's3://' + params['bucket'] + '/' + params['path']
      }
   )
   return response


def athena_to_s3(session, params, max_execution=5):
   client = session.client('athena', region_name=params["region"])
   execution = athena_query(client, params)
   execution_id = execution['QueryExecutionId']
   state = 'RUNNING'

   while max_execution > 0 and state in ['RUNNING', 'QUEUED']:
      max_execution = max_execution - 1
      response = client.get_query_execution(QueryExecutionId=execution_id)

      if 'QueryExecution' in response and \
              'Status' in response['QueryExecution'] and \
              'State' in response['QueryExecution']['Status']:
         state = response['QueryExecution']['Status']['State']
         if state == 'FAILED':
            return False
         elif state == 'SUCCEEDED':
            s3_path = response['QueryExecution']['ResultConfiguration']['OutputLocation']
            filename = re.findall('.*\/(.*)', s3_path)[0]
            return filename
      time.sleep(1)

   return False


def cleanup(session, params):
   s3 = session.resource('s3', aws_access_key_id='[AWS ACCESS KEY]',
                         aws_secret_access_key='[AWS SECRET KEY]')
   my_bucket = s3.Bucket(params['bucket'])
   for item in my_bucket.objects.filter(Prefix=params['path']):
      item.delete()


def getMoreCommon(request):
   params['query'] = 'SELECT * FROM ' + db + ' ORDER BY count DESC LIMIT 30'
   table = query()
   return render(request, 's3_website/athena.html', {'result' : table})

def getLessCommon(request):
   params['query'] = 'SELECT * FROM ' + db + ' ORDER BY count LIMIT 15'
   table = query()
   return render(request, 's3_website/athena.html', {'result' : table})

def getAWords(request):
   params['query'] = "SELECT * FROM " + db + " WHERE word LIKE 'a%' ORDER BY count DESC LIMIT 30"
   table = query()
   return render(request, 's3_website/athena.html', {'result' : table})

def getThreeDigitWords(request):
   params['query'] = "SELECT * FROM " + db + " WHERE LENGTH(word) = 3 ORDER BY count DESC LIMIT 30"
   table = query()
   return render(request, 's3_website/athena.html', {'result' : table})

def getIngWords(request):
   params['query'] = "SELECT * FROM " + db + " WHERE word LIKE '%ing' ORDER BY count DESC LIMIT 15"
   table = query()
   return render(request, 's3_website/athena.html', {'result' : table})

def getBigWords(request):
   params['query'] = "SELECT * FROM " + db + " WHERE LENGTH(word) > 10 ORDER BY count DESC LIMIT 15"
   table = query()
   return render(request, 's3_website/athena.html', {'result' : table})

def getFreqWords(request):
   params['query'] = "SELECT word, CAST((count/" + str(n_rows) + ".0)*100.0 AS DECIMAL(18,1)) AS Percentage FROM " + db + " ORDER BY count DESC LIMIT 30"
   table = query()
   return render(request, 's3_website/athena.html', {'result' : table})

def getCustomQ(request):
   if request.method == 'POST':
      form = PostForm(request.POST)
      q = request.POST['query_text']
      params['query'] = str(q)
      table = query()
      return render(request, 's3_website/athena.html', {'result': table})
   else:
      form = PostForm()
   return render(request, 's3_website/athena.html')

def query():
   session = boto3.Session()
   s3_filename = athena_to_s3(session, params)
   time.sleep(2)
   find_file = 's3://' + params['bucket'] + '/' + params['path']
   client = boto3.client('s3',
                         aws_access_key_id='[AWS ACCESS KEY]',
                         aws_secret_access_key='[AWS SECRET KEY]', )
   # Create the S3 object
   obj = client.get_object(
      Bucket='[BUCKET TO SAVE RESULTS]',
      Key='Unsaved/2021/output/' + str(s3_filename)
   )
   print('Unsaved/2021/output/' + str(s3_filename))
   table = pandas.read_csv(obj['Body']).to_html()
   return table

'''
session = boto3.Session()

s3_filename = athena_to_s3(session, params)

cleanup(session, params)
'''