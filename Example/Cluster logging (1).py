# Databricks notebook source
# MAGIC %sql
# MAGIC -- create database cluster_report location 'abfss://2021@stcampusbatch.dfs.core.windows.net/database/cluster_report'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Start time
# MAGIC  Finding midnight time for previous day in UNIX

# COMMAND ----------

import pytz
import time
from datetime import datetime, timedelta

### Previous days midnight (FINAL)
previous_day_midnight = (datetime.today() + timedelta(days=-1)).replace(hour=0, minute=0, second=0, microsecond=0).astimezone(pytz.utc).timetuple()
start_time = int(time.mktime(previous_day_midnight)*1000)
print(time.strftime("%Y-%m-%d %H:%M:%S",previous_day_midnight))
print(start_time)

### Previous Midnight
# previous_midnight = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).astimezone(pytz.utc).timetuple()
# start_time = int(time.mktime(previous_midnight)*1000)
# print(start_time)

### 24 hours ago from current time
# last_hour_date_time = (datetime.now() - timedelta(days = 1)).astimezone(pytz.utc).timetuple()
# start_time = int(time.mktime(last_hour_date_time)*1000)
# print(last_hour_date_time, start_time)

# COMMAND ----------

# MAGIC %md
# MAGIC ### End time
# MAGIC Finding midnight time for current day in UNIX

# COMMAND ----------

import datetime

### tonight's midnight (FINAL)
tonight_midnight = (datetime.datetime.today()).replace(hour=0, minute=0, second=0,microsecond=0).astimezone(pytz.utc).timetuple()
end_time = int(time.mktime(tonight_midnight)*1000)
print(time.strftime("%Y-%m-%d %H:%M:%S", tonight_midnight))
print (end_time)

## Current time
# from datetime import datetime, timedelta
# current_time = datetime.now().astimezone(pytz.utc).timetuple()
# end_time = int(time.mktime(current_time)*1000)
# print(last_hour_date_time, end_time)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generating json to pass as parameter

# COMMAND ----------

json_params_list=[{"cluster_id": "0802-100145-blurt686","start_time": start_time, "end_time": end_time, "event_type": ["STARTING", "TERMINATING"]}]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generating Cluster Activity for Previous day

# COMMAND ----------

import json, os, datetime, requests
import requests.packages.urllib3

global pprint_j
requests.packages.urllib3.disable_warnings()

cluster_logs=[]

for json_params in json_params_list:
#   print(type(json_params))
      

      # Helper to pretty print json
      def pprint_j(i):
          print(json.dumps(i, indent=4, sort_keys=True))


      class dbclient:
          """
          Rest API Wrapper for Databricks APIs
          """
          # set of http error codes to throw an exception if hit. Handles client and auth errors
          http_error_codes = (401, 403)

          def __init__(self, token, url):
              self._token = {'Authorization': 'Bearer {0}'.format(token)}
              self._url = url.rstrip("/")
              self._is_verbose = False
              self._verify_ssl = False
              if self._verify_ssl:
                  # set these env variables if skip SSL verification is enabled
                  os.environ['REQUESTS_CA_BUNDLE'] = ""
                  os.environ['CURL_CA_BUNDLE'] = ""

          def is_aws(self):
              return self._is_aws

          def is_verbose(self):
              return self._is_verbose

          def is_skip_failed(self):
              return self._skip_failed

          def test_connection(self):
              # verify the proper url settings to configure this client
              if self._url[-4:] != '.com' and self._url[-4:] != '.net':
                  print("Hostname should end in '.com'")
                  return -1
              results = requests.get(self._url + '/api/2.0/clusters/spark-versions', headers=self._token,
                                     verify=self._verify_ssl)
              http_status_code = results.status_code
              if http_status_code != 200:
                  print("Error. Either the credentials have expired or the credentials don't have proper permissions.")
                  print("If you have a ~/.netrc file, check those credentials. Those take precedence over passed input.")
                  print(results.text)
                  return -1
              return 0

          def get(self, endpoint, json_params=None, version='2.0', print_json=False):
              if version:
                  ver = version
              full_endpoint = self._url + '/api/{0}'.format(ver) + endpoint
              if self.is_verbose():
                  print("Get: {0}".format(full_endpoint))
              if json_params:
                  raw_results = requests.get(full_endpoint, headers=self._token, params=json_params, verify=self._verify_ssl)
                  http_status_code = raw_results.status_code
                  if http_status_code in dbclient.http_error_codes:
                      raise Exception("Error: GET request failed with code {}\n{}".format(http_status_code, raw_results.text))
                  results = raw_results.json()
              else:
                  raw_results = requests.get(full_endpoint, headers=self._token, verify=self._verify_ssl)
                  http_status_code = raw_results.status_code
                  if http_status_code in dbclient.http_error_codes:
                      raise Exception("Error: GET request failed with code {}\n{}".format(http_status_code, raw_results.text))
                  results = raw_results.json()
              if print_json:
                  print(json.dumps(results, indent=4, sort_keys=True))
              if type(results) == list:
                  results = {'elements': results}
              results['http_status_code'] = raw_results.status_code
              return results

          def http_req(self, http_type, endpoint, json_params, version='2.0', print_json=False, files_json=None):
              if version:
                  ver = version
              full_endpoint = self._url + '/api/{0}'.format(ver) + endpoint
              if self.is_verbose():
                  print("{0}: {1}".format(http_type, full_endpoint))
              if json_params:
                  if http_type == 'post':
                      if files_json:
                          raw_results = requests.post(full_endpoint, headers=self._token,
                                                      data=json_params, files=files_json, verify=self._verify_ssl)
                      else:
                          raw_results = requests.post(full_endpoint, headers=self._token,
                                                      json=json_params, verify=self._verify_ssl)
                  if http_type == 'put':
                      raw_results = requests.put(full_endpoint, headers=self._token,
                                                 json=json_params, verify=self._verify_ssl)
                  if http_type == 'patch':
                      raw_results = requests.patch(full_endpoint, headers=self._token,
                                                   json=json_params, verify=self._verify_ssl)

                  http_status_code = raw_results.status_code
                  if http_status_code in dbclient.http_error_codes:
                      raise Exception("Error: {0} request failed with code {1}\n{2}".format(http_type,
                                                                                            http_status_code,
                                                                                            raw_results.text))
                  results = raw_results.json()
              else:
                  print("Must have a payload in json_args param.")
                  return {}
              if print_json:
                  print(json.dumps(results, indent=4, sort_keys=True))
              # if results are empty, let's return the return status
              if results:
                  results['http_status_code'] = raw_results.status_code
                  return results
              else:
                  return {'http_status_code': raw_results.status_code}

          def post(self, endpoint, json_params, version='2.0', print_json=False, files_json=None):
              return self.http_req('post', endpoint, json_params, version, print_json, files_json)

          def put(self, endpoint, json_params, version='2.0', print_json=False):
              return self.http_req('put', endpoint, json_params, version, print_json)

          def patch(self, endpoint, json_params, version='2.0', print_json=False):
              return self.http_req('patch', endpoint, json_params, version, print_json)

          @staticmethod
          def my_map(F, items):
              to_return = []
              for elem in items:
                  to_return.append(F(elem))
              return to_return

          def set_export_dir(self, dir_location):
              self._export_dir = dir_location

          def get_export_dir(self):
              return self._export_dir

          def get_latest_spark_version(self):
              versions = self.get('/clusters/spark-versions')['versions']
              v_sorted = sorted(versions, key=lambda i: i['key'], reverse=True)
              for x in v_sorted:
                  img_type = x['key'].split('-')[1][0:5]
                  if img_type == 'scala':
                      return x

      class client(dbclient):

          def cluster_info(self):
            clusters_starting = self.post('/clusters/events/', json_params=json_params)
            return (clusters_starting)

      url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None) 
      token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)

      client = client(token, url)

      cluster_logs.append(client.cluster_info())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sorting and filtering list of cluster activity

# COMMAND ----------

from operator import itemgetter

cluster_logs_sorted = list()
cluster_logs_filtered = list()

### SORTING BASED ON TIMESTAMP

for i in range(0,len(cluster_logs)):
  try:
    cluster_logs_sorted.append(sorted(cluster_logs[i]['events'], key=itemgetter('timestamp')))
  except Exception as E:
    cluster_logs_sorted.append([])
### FILTERING REQUIRED ACTIVITIES

type_list = ['STARTING','TERMINATING']

for j in range(0,len(cluster_logs_sorted)):
  cluster_logs_filtered.append([log for log in cluster_logs_sorted[j] if log['type'] in type_list])



for i in range(0,len(cluster_logs_filtered)):
  for j in cluster_logs_filtered[i]:
    print(j)
  
# cluster_logs_utc = [(time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(int(log['timestamp']))), log['type']) for log in cluster_logs_filtered]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculating total activity time for a cluster

# COMMAND ----------

import time
from datetime import date, timedelta, datetime

start_time=0
end_time=0
difference=0
total_active_time=0
activity_list = list()

clusters_list = ['cmpsdb']
previous_date = date.today() + timedelta(days=-1)

for item in range(0,len(cluster_logs_filtered)):
  
    for index, log in enumerate(cluster_logs_filtered[item]):
      if log['type']=='STARTING':
        start_time=log['timestamp']
        print("Start Time: " + str(time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(start_time/1000.0))))
      elif log['type']=='TERMINATING':
        end_time=log['timestamp']
        print("End Time: " + str(time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(end_time/1000.0))))

      if start_time !=0 and end_time !=0 and end_time >= start_time:
        difference = end_time - start_time
        print("Difference: " + str(difference))
        total_active_time += difference
        start_time = 0
        end_time = 0
        
      ##Account for started but currently running and not terminated
      if index==len(cluster_logs_filtered[item])-1 and log['type']=='STARTING':
        difference = int(round(time.time()*1000)) - start_time
        print("Difference (Cluster currently running): " + str(difference))
        total_active_time += difference
        start_time = 0

    print("Total Active Time: " + str(total_active_time))

    total_active_time = int(total_active_time)
    seconds=(total_active_time/1000)%60
    seconds = int(seconds)
    minutes=(total_active_time/(1000*60))%60
    minutes = int(minutes)
    hours=(total_active_time/(1000*60*60))%24
    total_active_time = str(int(round(hours,0))) + " Hrs: " + str(minutes) + " Mins: " + str(seconds) + " Secs "
    # print ("%d:%d:%d" % (hours, minutes, seconds))

    

    activity_list.append((previous_date, clusters_list[i], total_active_time))

spark.createDataFrame(activity_list, ['Date', 'Cluster_Name', 'Total_Active_Time']).display()
# spark.createDataFrame(activity_list, ['Date', 'Cluster_Name', 'Total_Active_Time']).write.mode("append").format("delta").saveAsTable("cluster_report.cluster_report_daily")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select * from 
# MAGIC DESCRIBE detail 
# MAGIC cluster_report.cluster_report_daily

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rough

# COMMAND ----------

# from datetime import datetime, timedelta, date
# print(date.today() + timedelta(days=-1))

# COMMAND ----------

# total_active_time = int(5192705)
# seconds=(total_active_time/1000)%60
# seconds = int(seconds)
# minutes=(total_active_time/(1000*60))%60
# minutes = int(minutes)
# hours=(total_active_time/(1000*60*60))%24
# # total_active_time = str(int(round(hours,0))) + " Hrs: " + str(minutes) + " Mins: " + str(seconds) + " Secs "
# print ("%d:%d:%d" % (hours, minutes, seconds))

# COMMAND ----------

# import time
# print(time.time()*1000.0)

# COMMAND ----------

# MAGIC %py
# MAGIC # token="dapi4e68ce2f165af537c11db5c105a4934a-3"
# MAGIC # token = {'Authorization': 'Bearer {0}'.format(token)}
# MAGIC # json={
# MAGIC #   "cluster_id": "0802-100145-blurt686",
# MAGIC #   "end_time": 1650623550930,
# MAGIC #   "order": "DESC",
# MAGIC #   "offset": 10,
# MAGIC #   "limit": 20,
# MAGIC #   "event_type": "STARTING"
# MAGIC # }
# MAGIC 
# MAGIC # # print(token)
# MAGIC # import requests
# MAGIC # requests.put("https://adb-6957949823637042.2.azuredatabricks.net/api/2.0/clusters/events", headers=token, json={"cluster_id": "0802-100145-blurt686", "event_type": "STARTING"}, verify=False)

# COMMAND ----------

# MAGIC %py
# MAGIC # dbutils.fs.ls('dbfs:/cluster-logs/0802-100145-blurt686/eventlog/0802-100145-blurt686_10_2_12_7/8966879715202257696/')
# MAGIC # spark.read.csv('dbfs:/cluster-logs/0802-100145-blurt686/eventlog/0802-100145-blurt686_10_2_12_7/8966879715202257696/eventlog-2022-02-03--09-00.gz').display()
# MAGIC # spark.read.csv('dbfs:/cluster-logs/0802-100145-blurt686/eventlog/0802-100145-blurt686_10_2_12_7/8966879715202257696/eventlog/',header=True).display()

# COMMAND ----------

# MAGIC %py
# MAGIC # import time
# MAGIC # ti = 1650527576196
# MAGIC # print(time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(1650527576196/1000.)))

# COMMAND ----------

# import time

# start_time=0
# end_time=0
# difference=0
# total_active_time=0

# clusters_list = ['cmpsdb']

# for index, log in enumerate(cluster_logs_filtered):
#   if log['type']=='STARTING':
#     start_time=log['timestamp']
#     print("Start Time: " + str(start_time))
#   elif log['type']=='TERMINATING':
#     end_time=log['timestamp']
#     print("End Time: " + str(end_time))
  
#   if start_time !=0 and end_time !=0 and end_time >= start_time:
#     difference = end_time - start_time
#     print("Difference: " + str(difference))
#     total_active_time += difference
#     start_time = 0
#     end_time = 0
    
#   if index==len(cluster_logs_filtered)-1:
#     difference = int(time.time()) - start_time
#     print("Difference (Cluster currently running): " + str(difference))
#     total_active_time += difference
#     start_time = 0
  
# print("Total Active Time: " + str(total_active_time))

# total_active_time = int(total_active_time)
# seconds=(total_active_time/1000)%60
# seconds = int(seconds)
# minutes=(total_active_time/(1000*60))%60
# minutes = int(minutes)
# hours=(total_active_time/(1000*60*60))%24
# total_active_time = str(int(round(hours,0))) + " Hrs: " + str(minutes) + " Mins: " + str(seconds) + " Secs "
# # print ("%d:%d:%d" % (hours, minutes, seconds))

# ls = list()

# ls.append((clusters_list[i], total_active_time))

# spark.createDataFrame(ls, ['Cluster Name', 'Total Active Time']).display()
# # print( "Total Activity Time: \n cmpsdb cluster: " + total_active_time + " Hrs")
# ##Account for started but currently running and not terminated
