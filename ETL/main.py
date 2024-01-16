from extract import extractor_post_metada
from transform import transform_posts_metadata
from load import load_post_metadata

from extract import extractor_hashtag
from transform import transform_posts_hashtag
from load import load_post_hashtags


def main():
    # Initializing ETL process for hashtags table
    df = extractor_hashtag()

    df = transform_posts_hashtag(df)

    load_post_hashtags(df)

    # Initializing ETL process for posts metadata table
    df = extractor_post_metada()

    df = transform_posts_metadata(df)

    load_post_metadata(df)


if __name__ == "__main__":
    main()