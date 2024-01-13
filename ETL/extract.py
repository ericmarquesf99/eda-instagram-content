import instaloader
import pandas as pd

def extract_post_metadata(post):
    return {
        'post_id': post.mediaid,
        'qt_likes': post.likes,
        'qt_comments': post.comments,
        'post_date': post.date,
        'video': post.is_video,
        'video_duration': post.video_duration,
        'qt_views': post.video_view_count,
        'caption': post.caption,
        'hashtags': post.caption_hashtags
    }

def extract_post_hashtags(post):
    return {
        'post_id': post.mediaid,
        'hashtags': post.caption_hashtags
    }

def extractor():
    loader = instaloader.Instaloader()

    # Extract profile posts metadata
    profile = instaloader.Profile.from_username(loader.context, 'gerandoafetospsi')
    posts_metadata = [extract_post_metadata(post) for post in profile.get_posts()]

    # Saving metadata in Pandas DataFrames
    df = pd.DataFrame(posts_metadata)
    return df

    # Save DataFrames in CSV files
    # posts_df.to_csv('CSV/posts_metadata.csv', index=False, sep= ";", encoding="utf-8")


def extractor_hashtag():
    loader = instaloader.Instaloader()

    # Extract profile posts metadata
    profile = instaloader.Profile.from_username(loader.context, 'gerandoafetospsi')
    posts_hashtag = [extract_post_hashtags(post) for post in profile.get_posts()]

    # Saving metadata in Pandas DataFrames
    df = pd.DataFrame(posts_hashtag)

    return df