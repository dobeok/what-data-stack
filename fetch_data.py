from dotenv import load_dotenv
import os
import pandas as pd
import praw
import re

load_dotenv()

client_id = os.getenv('CLIENT_ID')
client_secret = os.getenv('CLIENT_SECRET')
user_agent = os.getenv('USER_AGENT')


reddit = praw.Reddit(client_id=client_id, client_secret=client_secret, user_agent=user_agent)
url = 'https://www.reddit.com/r/dataengineering/comments/wcw0nt/what_is_in_your_data_stack_thread/'
submission = reddit.submission(url=url)


# fetch comment data from reddit
data = {}
for idx, top_level_comment in enumerate(submission.comments):
    data[idx] = {
        'created_utc': top_level_comment.created_utc,
        'author': top_level_comment.author,
        'score': top_level_comment.score,
        'body': top_level_comment.body,
        }


# to map numbered list items
list_items_map = {
    1:'ETL',
    2:'Data Warehouse',
    3:'Data Transformation',
    4:'BI',
    5:'Exploratory Data Analysis',
    6:'Company Size',
    7:'Company Industry',
    8:'Company HQ',
}


# identify numbered list and extract relevant data
def parse_list(text):
    """
    lists with bullets: number, '*', or '-'
    """
    list_pattern = re.compile('([1-8])\.?\s?(.*)')
    rows = text.split('\n')

    result = {}
    
    for row in rows:
        list_content = list_pattern.search(row)
        if list_content:
            _key = list_content.group(1).strip()
            _value = list_content.group(2).strip()

            
            result[list_items_map[int(_key)]] = _value

    return result


# clean comment body
for k, v in data.items():
    v['body'] = parse_list(v['body'])


# prepare dataframe for easier aggregating
df = pd.DataFrame(data).T
df = pd.concat([df, pd.json_normalize(df['body'])], axis=1).drop('body', axis=1)
df['score'] = pd.to_numeric(df['score'])


if __name__ == '__main__':
    df.to_csv('data/cleaned.csv', index=False)