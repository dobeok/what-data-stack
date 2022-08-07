
import pandas as pd
import difflib
from collections import Counter


with open('data/1-1000.txt') as file:
    english_words = file.readlines()
    english_words = [line.rstrip() for line in english_words]


tools = {
    'ETL': ['Azure Data Factory', 'Google Cloud Dataflow', 'AWS Glue', 'Dataddo', 'Hadoop', 'Airflow'],
    'Data Warehouse': ['Amazon Redshift', 'Microsoft Azure', 'Google BigQuery', 'Snowflake', 'Micro Focus Vertic', 'Teradata', 'Amazon DynamoDB', 'PostgreSQL',],
    'Data Transformation': ['dbt', 'EasyMorph', 'Dataform', 'Matillion'],
    'BI': ['SAP', 'Datapine', 'MicroStrategy', 'SAS', 'Qlik', 'Zoho', 'Sisense', 'Power BI', 'Looker', 'Tableau', 'Oracle, '],
    'Exploratory Data Analysis': ['Databricks', 'Colab', 'Jupyter', 'Mode', 'SQL'],
}


all_tools = ['Cron', 'Spark', 'Python', 'Excel', 'Airflow'] 
for tool in tools.values():
    all_tools += tool


# remove comments with score <= 0 to avoid spam/low quality answers
df = pd.read_csv('data/cleaned.csv')
df = df[df['score'] > 0]


def find_match(list_to_search, li):
    result = []
    
    if len(list_to_search):
        for item in li:
            if item.lower().strip() in list_to_search:
                result.append(item)
    
    return result


def simpify_list(li):
    """
    initialize the first element with the most common response;
    then for each subsequent element, decide to create a new group or not based on similarity
    score with each element in the current list
    """
    counter = Counter(li)
    most_common_val = counter.most_common(1)
    result = [most_common_val[0][0]]

    for el in li:
        closest_matches = difflib.get_close_matches(el, result)
        if not len(closest_matches):
            # new topic
            result.append(el)
    
    return result


def fold_list(items):
    """
    remove duplicated items due to different capitalizations
    """
    items = [item.capitalize() for item in items]
    items = list(set(items))
    return items


def zip_scores(score, li):
    """
    ziping scores and values from 2 columns
    """
    result = []
    for item in li:
        result.append({item: score})
    
    return result


for orig_col in tools.keys():

    new_col = f'{orig_col}_clean'
    df[new_col] = df[orig_col].str.lower()
    df[new_col] = df[new_col].fillna('')
    df[new_col] = df[new_col].str.replace(r"\(.*\)","", regex=True)
    df[new_col] = [item.replace('/', ',').replace(' and ', ',').replace(' or ', ',').replace('+', ',').replace('&', ',') for item in df[new_col]]
    df[new_col] = [''.join(ch for ch in item if (ch.isalnum() or ch in [' ', ','])) for item in df[new_col]]

    # remove common english word
    df[new_col] = df[new_col].str.lower()
    for word in english_words:
        df[new_col] =  df[new_col].str.replace(f' {word} ', ' ')

    df[new_col] = df[new_col].str.replace(', ', ',').str.replace(' ,',',')
    df[new_col] = [item.split(',') for item in df[new_col]]



    df['iter1'] = df[new_col].apply(find_match,li=all_tools)

    # grouping unsorted comments
    unsorted = df[df["iter1"].str.len() == 0][new_col]
    unsorted = [item for sublist in unsorted for item in sublist]
    unsorted = [item.strip() for item in unsorted]
    unsorted_short = simpify_list(unsorted)

    df['iter2'] = df[new_col].apply(find_match, li=unsorted_short)
    df[new_col] = df['iter1'] + df['iter2']
    df = df.drop(['iter1', 'iter2'], axis=1)
    df[new_col] = df[new_col].apply(fold_list)
    
    df[f'{orig_col}_weight'] = df.apply(lambda x: zip_scores(x['score'], x[new_col]), axis=1)


data = {}
for orig_col in tools.keys():
    values  = [item for sublist in df[f'{orig_col}_weight'] for item in sublist]
    
    counter = Counter()
    for d in values: 
        counter.update(d)
    
    result = dict(counter)
    data[orig_col] = result


dfc = pd.DataFrame(data)
dfc = dfc.reset_index()
dfc = dfc.rename(columns={'index': 'tech'})
dfc['one'] = 1
dfc['tech_no_space'] = dfc['tech'].str.replace(' ','')
dfcg = dfc.groupby(dfc['tech_no_space']).sum()


# group similar names with different spacing
mapper = dfc.drop(dfc.loc[(~dfc['tech'].str.contains(' ')) & (dfc['tech']!=dfc['tech_no_space'])].index)[['tech', 'tech_no_space']]
mapper_dict = pd.Series(mapper['tech'].values, mapper['tech_no_space'].values).to_dict()

dfcg['tech'] = dfcg.index.map(mapper_dict)
dfcg = dfcg.drop(dfcg.loc[dfcg['tech']==''].index)
dfcg = dfcg.set_index('tech')
dfcg['total'] = dfcg.sum(axis=1)


if __name__ == '__main__':
    df.to_csv('data/intermediate.csv', index=False)
    dfcg.to_csv('data/processed.csv')