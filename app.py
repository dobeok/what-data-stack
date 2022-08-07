import plotly.express as px
import streamlit as st
import pandas as pd
from PIL import Image

st.set_page_config(
     page_title="Analyze reddit comments",
    #  layout="wide",
     initial_sidebar_state="expanded",
)



df = pd.read_csv('data/processed.csv').reset_index()
st.title('What is your Data Stack? Analysis from Reddit thread')
st.markdown('**Keywords:** `Reddit` `PRAW` `Visualization`')
st.markdown("""
##  Summary
I came across [this thread](https://www.reddit.com/r/dataengineering/comments/wcw0nt/what_is_in_your_data_stack_thread/)
whereby people were sharing their data stack (ETL, Data Warehouse, EDA, etc) and it piqued my curiousity.
Initially, I did  a CTRL+F search of the tech and count the number of results, but then I thought this process might come in handy
later on when I want to analyze more complex threads; or perhaps some day I will might share my findings with someone else.
That's why I decided to make a web app using streamlit as practice.
""")


st.image(Image.open('resources/drawio.png'))

columns = [
    'ETL',
    'Data Warehouse',
    'Data Transformation',
    'BI',
    'Exploratory Data Analysis'
]


st.markdown("""
## How I did it

- Step 1: Use [PRAW](https://praw.readthedocs.io/en/stable/) to query from reddit, and save to txt file
- Step 2: Use pandas to process and create tabular data
- Step 3: [Streamlit](https://streamlit.io/) for front-end
[See full code]()
""")

st.markdown("""
#### Observations:
- Most of the comment are nicely structured in 1-8 numbered bullet points. So I can parse with regex.
- The same technology can be written in a few different ways, for example: Power BI, PBI. But using text similarity match might yield some inaccurate matching, for example: Amazon XX vs Amazon YY. Hence the safest solution is to list out common tech, and do an exact match first.
- Perhaps ranking by raw comment count is not accurate. This is because when people see answers that are similar to their own, they tend to just upvote the comment instead of making a new one. That's why I decided to tally the tech by the sum of scores.

""")

st.markdown('## Results')

for col in columns:
    tb = df.sort_values(by=col, ascending=False)
    score = tb[['tech', col]].head(6)
    
    fig = px.bar(x=score['tech'], y=score[col] , orientation='v', height=320, title=f'<b>{col}</b>', template='simple_white')
    fig.update_layout(title_x=0.5, xaxis_title='', yaxis_title='Sum of scores')
    fig.update_layout(yaxis={'visible': True, 'showticklabels': True, 'matches':None})
    fig.update_layout(margin_b=0, margin_t=50, margin_l=0, margin_r=0)
    fig.update_layout(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
    
    st.plotly_chart(fig, use_container_width=True)


st.markdown('## End')