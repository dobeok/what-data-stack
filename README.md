###  Summary
I came across [this thread](https://www.reddit.com/r/dataengineering/comments/wcw0nt/what_is_in_your_data_stack_thread/) whereby people were sharing their data stack (ETL, Data Warehouse, EDA, etc) and it piqued my curiousity.

Initially, I did  a CTRL+F search of the tech and count the number of results, but then I thought this process might come in handy later on when I want to analyze more complex threads; or perhaps some day I will might share my findings with someone else.

That's why I decided to make a web app using streamlit as practice.

_Visualizing input and output_
![from comments to table](/resources/drawio.png)


### Observations
- Most of the comment are nicely structured in 1-8 numbered bullet points. So I can parse with regex.
- The same technology can be written in a few different ways, for example: Power BI, PBI. But using text similarity match might yield some inaccurate matching, for example: Amazon XX vs Amazon YY. Hence the safest solution is to list out common tech, and do an exact match first.
- Perhaps ranking by raw comment count is not accurate. This is because when people see answers that are similar to their own, they tend to just upvote the comment instead of making a new one. That's why I decided to tally the tech by the sum of scores.

### Sample charts
![ETL](/resources/plot-etl.png)

![ETL](/resources/plot-bi.png)



### Use
To run the web app using **old data**

```bash
streamlit run app.py
```

**To update data**, you will need to get an api key from reddit, save them in the .env file
```bash
CLIENT_ID={your_client_id}
SECRET={your_client_secret}
USER_AGENT={your_user_agent}
```

and then
```bash
python fetch_data.py
python clean_data.py
streamlit run app.py
```
