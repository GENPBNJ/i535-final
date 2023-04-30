#%%
import base64
import datetime
import pandas as pd
from google.cloud import storage
import matplotlib.pyplot as plt
import os

# Define variables for Cloud Functions
bucket_name = 'bigquery_exports_final_i535'
project_name = 'i535-final-project-384623'
os.environ["GCLOUD_PROJECT"] = project_name

def generate_visualization():
    """Write and read a blob from GCS using file-like IO"""

    # The ID of your new GCS object
    blob_name = "topnewsstories000000000000.csv"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    df = pd.read_csv('gs://' + bucket_name + '/' + blob_name, encoding='utf-8')


    df = df.sort_values('height_traffic')
    titles = list(df['title'])
    dates = list(df['published_date'])
    for i in range(len(dates)):
      dates[i] = dates[i][8:16]
    
    x_axis = [None] * len(dates)
    for i in range(len(dates)):
      x_axis[i] = titles[i] + ', ' + dates[i]
    y_axis = list(df['height_traffic'])

    plt.barh(x_axis, y_axis)
    plt.title('Most Viewed Stories')
    plt.xlabel('Number of Views')
    plt.ylabel('Story Name and Date')
    ax = plt.gca()
    ax.get_xaxis().get_major_formatter().set_scientific(False)
    plt.savefig('top_stories_numbers.png', bbox_inches='tight')
    plt.show()

    # Get the current time
    today = datetime.datetime.now().strftime('%Y-%m-%d%H:%M:%S')

    # Upload CSV file to Cloud Storage
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob('top_stories_numbers.png')
    blob.upload_from_filename('top_stories_numbers.png')


def helloGCS(event, context):
    gcs_message = base64.b64decode(event['data']).decode('utf-8')
    print(gcs_message)
    generate_visualization()

if __name__ == "__main__":
    helloGCS('data', 'context')
# %%
