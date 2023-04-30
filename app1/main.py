#%%
import base64
import datetime
import pandas as pd
from google.cloud import storage
from google.cloud import bigquery
import feedparser
import matplotlib.pyplot as plt


# Define variables for Cloud Functions
bucket_name = 'i535-bucket-data'
project_name = 'i535-final-project-384623'
dataset_name = 'test'
table_name = 'example_data'

def generate_data():
    # Generate data
    url = "https://trends.google.com/trends/trendingsearches/daily/rss?geo=US"

    feed = feedparser.parse(url)

    titles = []
    height_approx_traffic = []
    description = []
    published = []
    ht_news_item_url = []
    ht_news_item_title = []
    news_source = []

    for entry in feed.entries:
        titles.append(entry.title)
        height_approx_traffic.append(entry.ht_approx_traffic)
        description.append(entry.description)
        published.append(entry.published)
        ht_news_item_url.append(entry.ht_news_item_url)
        ht_news_item_title.append(entry.ht_news_item_title)
        news_source.append(entry.ht_news_item_source)

    data = {'Titles': titles, 'Height Traffic': height_approx_traffic, 'Description':description,'Published Date':published, 'Link':ht_news_item_url, 'News Item Title':ht_news_item_title, 'News Source': news_source}
    df = pd.DataFrame(data)
    df.fillna('N/A', inplace=True)
    df['Height Traffic'] = df['Height Traffic'].str.replace('+','').str.replace(',','')
    df = df.astype({'Height Traffic': int})

    df = df.sort_values('Height Traffic')
    titles = list(df['Titles'])
    dates = list(df['Published Date'])
    for i in range(len(dates)):
      dates[i] = dates[i][8:16]
    
    x_axis = [None] * len(dates)
    for i in range(len(dates)):
      x_axis[i] = titles[i] + ', ' + dates[i]
    y_axis = list(df['Height Traffic'])

    plt.barh(x_axis, y_axis)
    plt.title('Most Viewed Stories')
    plt.xlabel('Number of Views')
    plt.ylabel('Story Name and Date')
    ax = plt.gca()
    ax.get_xaxis().get_major_formatter().set_scientific(False)
    plt.savefig('top_stories_numbers.png', bbox_inches='tight')
    plt.show()


    # Convert the DataFrame to a CSV string
    csv_string = df.to_csv(index=False, header=False)

    # Get the current time
    today = datetime.datetime.now().strftime('%Y-%m-%d%H:%M:%S')

    # Upload CSV file to Cloud Storage
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(f'cloud_function_data/example_data_{today}.csv')
    blob.upload_from_string(csv_string)
    blob = bucket.blob(f'cloud_function_plots/top_stories_numbers_{today}.png')
    blob.upload_from_file('top_stories_numbers.png')


    # Upload the CSV file from Cloud Storage to BigQuery
    client = bigquery.Client()
    table_id = project_name + '.' + dataset_name + '.' + table_name
    job_config = bigquery.LoadJobConfig(
      autodetect=True,
      source_format=bigquery.SourceFormat.CSV,
      write_disposition='WRITE_TRUNCATE'
    )
    uri = f"gs://{bucket_name}/{blob.name}"
    load_job = client.load_table_from_uri(
      uri, table_id, job_config=job_config
    )  
    load_job.result()  

    # Make an API request and display number of loaded rows
    destination_table = client.get_table(table_id) 
    print("Loaded {} rows.".format(destination_table.num_rows))

def hello_pubsub(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    print(pubsub_message)
    generate_data()

if __name__ == "__main__":
    hello_pubsub('data', 'context')
# %%
