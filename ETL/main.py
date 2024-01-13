from extract    import extractor_hashtag
from transform  import transform_posts_hashtag
from load       import load_post_hashtags

def main():
    # Chama extract_data para obter o DataFrame
    df = extractor_hashtag()

    df = transform_posts_hashtag(df)

    load_post_hashtags(df)

if __name__ == "__main__":
    main()