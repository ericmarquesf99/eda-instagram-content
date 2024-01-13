import pandas as pd

def transform_posts_metadata(df):
    # Initiating data cleaning and data modeling

    ## Replacing "," for "." in the number format
    df['qt_views'] = df['qt_views'].replace('.',',')

    ## Replacing null values of qt_views columns with 0 as converting it to integer
    df['qt_views'] = df['qt_views'].fillna(0)
    df['qt_views'] = df['qt_views'].astype(int)

    ## Replacing TRUE or FALSE for Yes or NO
    df['video'] = df['video'].replace('True','Yes')
    df['video'] = df['video'].replace('False','No')

    return df

def transform_posts_hashtag(df):

    # Generating one row per POST_ID for each hashtag
    df = df.explode('hashtags')
    
    return df