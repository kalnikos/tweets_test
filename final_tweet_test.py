import pandas as pd
import snscrape.modules.twitter as twitter
from datetime import datetime, timedelta
import pyodbc
import datetime



### query format function
def format_query(y,x):
    search = 'from:{} since:{}'
    url = search.format(y, x)
    return url

### search day function (1 week back)
def date_fun():
    week_back = datetime.date.today() - timedelta(days=7)
    week_back = str(week_back.strftime('%Y-%m-%d'))
    return week_back

## extract data function
def  tweets_table():
    ### list of accounts
    pages = ['hnwines', 'BidfoodUK', 'BookerCatering', 'Brakes_Food', 'EnotriaCoe', 'InvMorton', 'liberty_wines', 'StAustellBrew']
    since_day = date_fun()
    tweets_list = []
    for page in pages:
        for tweet in (twitter.TwitterSearchScraper(format_query(page, since_day)).get_items()):
            tweets_list.append([tweet.user.username, tweet.id, tweet.date, tweet.content, 
                                tweet.url, tweet.cashtags, tweet.coordinates, 
                                tweet.inReplyToUser, tweet.replyCount, tweet.likeCount, tweet.retweetCount, 
                                tweet.inReplyToUser, tweet.outlinks, tweet.retweetedTweet])

            # Creating a dataframe from the tweets list above 
            tweets_df = pd.DataFrame(tweets_list, columns=['Username','Tweet_Id','Datetime','Text', 'url', 'hashtags', 'coordinates', 
                                                         'in_reply_user', 'reply_count', 'likes', 
                                                         'retweet_Count', 'Reply_To_User', 'outlinks', 'retweetedTweet'])
    
#     clean text/remove stopwords
    tweets_df['cleaned_content'] = tweets_df["Text"]#.apply(text_preprocessing)
    #tweets_df['number of words'] = tweets_df['cleaned_content'].apply(lambda x: len(str(x).split(' ')))
#     search for number of specific words 
    tweets_df['Beer'] = tweets_df['cleaned_content'].str.count('beer')
    tweets_df['Wine'] = tweets_df['cleaned_content'].str.count('wine')
    tweets_df['Price'] = tweets_df['cleaned_content'].str.count('price')
    tweets_df['Tweet_Id'] = tweets_df['Tweet_Id'].astype('int') 
        
    
    return tweets_df 

def load_df():
    df = tweets_table()

    conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                      'Server=localhost;'
                      'Database=master;'
                      'Trusted_Connection=yes;')

    cursor = conn.cursor()


## Insert only new rows
    query = """
                SELECT *
                FROM dbo.test_tweet
            """

## Query the product data 
    tweets = pd.read_sql(query, conn)
    
    ## find the new rows
    df_new = df.loc[~df['Tweet_Id'].isin(tweets.Tweet_Id)]

    final_df = df_new[["Username"
      ,"Tweet_Id"
      ,"Datetime"
      ,"Text"
      ]]
    
    
    # Prepare insert statement
    ## Inserting the brand info (How often should we update the table?)
    insert_sql_stmt = f"insert into dbo.test_tweet (Username, Tweet_Id, Datetime,Text ) values(?,?,?,?)"
    # Execute bulk statement
    cursor.executemany(insert_sql_stmt, final_df.values.tolist())
    # Close connection
    conn.commit()
load_df()