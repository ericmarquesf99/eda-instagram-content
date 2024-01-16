import numpy as np

# Function to replace single quotes inside hashtag column array
def replace_single_quotes(arr):
    return [s.replace("'", '"') for s in arr]

def transform_posts_metadata(df):
    # Initiating data cleaning and data modeling


    # Replacing "," for "." in the number format
    df['qt_views'] = df['qt_views'].replace('.',',')

    # Replacing null values of qt_views columns with 0 as converting it to integer
    df['qt_views'] = df['qt_views'].fillna(0)
    df['qt_views'] = df['qt_views'].astype(int)

    # Replacing TRUE or FALSE for Y or N
    df['video'] = df['video'].astype(str)
    df['video'] = df['video'].str.upper().replace('TRUE','Y')
    df['video'] = df['video'].str.upper().replace('FALSE','N')

    # Treating NaN values to be shown as Null values
    df['video_duration'] = df['video_duration'].replace({np.nan: None})

    # Removing single quotes for array elements in hashtags
    df['hashtags'] = df['hashtags'].apply(replace_single_quotes)
    return df

def transform_posts_hashtag(df):

    # Generating one row per POST_ID for each hashtag
    df = df.explode('hashtags')
    
    return df