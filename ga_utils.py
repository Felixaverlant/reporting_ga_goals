import argparse
from apiclient.discovery import build
import httplib2
from oauth2client import client
from oauth2client import file
from oauth2client import tools
import pandas as pd
import configparser
import os
import re

config = configparser.ConfigParser()
config.read('config.ini')

dir_path = os.path.dirname(os.path.realpath(__file__))

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
DISCOVERY_URI = ('https://analyticsreporting.googleapis.com/$discovery/rest')
CLIENT_SECRETS_PATH = dir_path+'/'+str(config['Creds']['CLIENT_SECRETS_PATH'])
VIEW_ID = config['Creds']['VIEW_ID']


def initialize_analyticsreporting():
  parser = argparse.ArgumentParser(
      formatter_class=argparse.RawDescriptionHelpFormatter,
      parents=[tools.argparser])
  flags = parser.parse_args([])

  flow = client.flow_from_clientsecrets(
      CLIENT_SECRETS_PATH, scope=SCOPES,
      message=tools.message_if_missing(CLIENT_SECRETS_PATH))

  storage = file.Storage('analyticsreporting.dat')
  credentials = storage.get()
  if credentials is None or credentials.invalid:
    credentials = tools.run_flow(flow, storage, flags)
  http = credentials.authorize(http=httplib2.Http())

  analytics = build('analytics', 'v4', http=http, discoveryServiceUrl=DISCOVERY_URI)

  return analytics

def get_report(analytics):
  return analytics.reports().batchGet(
      body={
        'reportRequests': [
        {
          'viewId': VIEW_ID,
          'dateRanges': [{'startDate': '2016-06-01', 'endDate': '2016-10-10'}],
          'metrics': [{'expression': 'ga:sessions'}],
          'dimensions': [{'name': 'ga:date'}]
        }]
      }
  ).execute()

def ga_to_df(v4_response):
    """Take a resp json from GA and transform to pandas DF """
    try:
        rows = v4_response['reports'][0]['data']['rows']
        header = v4_response['reports'][0]['columnHeader']
        index_col = header['dimensions']
        _ = index_col.extend([v.get('name') for v in header['metricHeader']['metricHeaderEntries']])
        index_col = [re.sub(r'ga:(.*)', r'\1', v) for v in index_col]
        _dims = [v.get('dimensions') for v in rows]
        _mets = [v.get('metrics')[0].get('values') for v in rows]
        _ = [u.extend(v) for u, v in zip(_dims, _mets)]
        df = pd.DataFrame(_dims, columns=index_col)
    except KeyError:
        df = pd.DataFrame({'error':pd.Series(['no data for this query'])})
    return df

def example():
    analytics = initialize_analyticsreporting()
    response = get_report(analytics)
    df = ga_to_df(response)
    return df

if __name__ == '__main__':
  example()
