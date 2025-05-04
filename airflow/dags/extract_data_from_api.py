def extract_data_from_api(**kwargs):

    from googleapiclient.discovery import build
    import os
    import pandas as pd
    from datetime import datetime
   


    

    def extract_data(api_key,region_codes,category_ids):

        youtube = build("youtube","v3",developerKey=api_key)

        video_data = []

        for region_code in region_codes:
            for category_id in category_ids:
                # Initialize the next_page_token to None for each region and category
                next_page_token = None
                while True:
                    # Make a request to the YouTube API to fetch trending videos
                    request = youtube.videos().list(
                        part='snippet,contentDetails,statistics',
                        chart='mostPopular',
                        regionCode=region_code,
                        videoCategoryId=category_id,
                        maxResults=50,
                        pageToken=next_page_token
                    )
                    response = request.execute()
                    videos = response['items']

                    # Process each video and collect data
                    for video in videos:
                        video_info = {
                            'region_code': region_code,
                            'category_id': category_id,
                            'video_id': video['id'],
                            'title': video['snippet']['title'],
                            'published_at': video['snippet']['publishedAt'],
                            'view_count': int(video['statistics'].get('viewCount', 0)),
                            'like_count': int(video['statistics'].get('likeCount', 0)),
                            'comment_count': int(video['statistics'].get('commentCount', 0)),
                            'channel_title': video['snippet']['channelTitle']
                        }
                        video_data.append(video_info)

                    # Get the next page token, if there are more pages of results
                    next_page_token = response.get('nextPageToken')
                    if not next_page_token:
                        break

        return pd.DataFrame(video_data)
    

    api_key = kwargs["api_key"]
    region_codes = kwargs["region_codes"]
    category_ids = kwargs["category_ids"]

    df_trending_video = extract_data(api_key,region_codes,category_ids)
    current_date = datetime.now().strftime("%Y%m%d")
    output_path = f'/opt/airflow/data/Youtube_Trending_Data_Raw_{current_date}.csv'
    df_trending_video.to_csv(output_path)




