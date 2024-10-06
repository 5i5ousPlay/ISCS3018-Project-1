import json
import re
from html import unescape
import pandas as pd
from googleapiclient.discovery import build
import nltk
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer


# Initialize NLTK processors
nltk.download('wordnet')
nltk.download('omw-1.4')
nltk.download('stopwords')
stopwords = stopwords.words('english')

lemmatizer = WordNetLemmatizer()


# Check for config file and API KEY
try:
    with open('config.json', 'r') as f:
        config = json.load(f)

        youtube = build('youtube', 'v3', developerKey=config['API_KEY'])
except KeyError:
    raise KeyError("Missing required key 'api_key' in config.json")
except FileNotFoundError:
    raise FileNotFoundError("The config.json file was not found. Please provide a valid configuration file.")


# EXTRACT FUNCTIONS
def get_playlist_videos(playlist_id: str):
    """
    Accepts a playlist id and returns the information of its videos.
    """
    return youtube.playlistItems().list(part='snippet,contentDetails', playlistId=playlist_id, maxResults=50).execute()


def extract_comments(video_id: str, video_title: str, video_date):
    """
    Accepts a video id, video title, and video date and returns a dataframe
    containing the comments and replies from the specified video. The function
    makes an API call using the provided video id and retrieves comments for as long as
    a 'nextPageToken' is found in the response body of the API call
    """
    comments = pd.DataFrame(columns=['video_title', 'video_id', 'video_date', 'text', 'comment_date'])
    video_response = youtube.commentThreads().list(part='snippet,replies',
                                                   videoId=video_id, order="relevance", maxResults=100).execute()
    requests_made = 1
    while 'nextPageToken' in video_response:
        for item in video_response['items']:
            comment = item['snippet']['topLevelComment']['snippet']['textDisplay']
            video = item['snippet']['topLevelComment']['snippet']['videoId']
            date = item['snippet']['topLevelComment']['snippet']['publishedAt']
            row = [video_title, video, video_date, comment, date]
            comments.loc[len(comments)] = row

            reply_count = item['snippet']['totalReplyCount']
            if (reply_count > 0) and ('replies' in item):
                for reply in item['replies']['comments']:
                    reply_text = reply['snippet']['textDisplay']
                    video_src = reply['snippet']['videoId']
                    r_date = reply['snippet']['publishedAt']
                    row = [video_title, video_src, video_date, reply_text, r_date]
                    comments.loc[len(comments)] = row

        requests_made += 1
        print(f"Next comment page found | EXTRACTING DATA | REQUESTS MADE: {requests_made} | COMMENT COUNT: {len(comments)}")
        # print(video_response['nextPageToken'])
        video_response = youtube.commentThreads().list(part='snippet,replies',
                                                        videoId=video_id,
                                                        order="relevance",
                                                        pageToken=video_response['nextPageToken']).execute()

    print(str(len(comments)) + ' comments extracted')
    return comments


def extract_playlist_comments(playlistId: str) -> pd.DataFrame:
    """
    Accepts a playlist id and returns a dataframe of all the comments collected
    from each video within the playlist.
    """
    comments = pd.DataFrame(columns=['video_title', 'video_id', 'video_date', 'text', 'comment_date'])
    playlist = get_playlist_videos(playlistId)
    for video in playlist['items']:
        video_id = video['contentDetails']['videoId']
        video_title = video['snippet']['title']
        video_date = video['snippet']['publishedAt']
        print(f"EXTRACTING FROM --> {video_title}")
        comments = pd.concat([comments, extract_comments(video_id, video_title, video_date)], axis=0, join='outer',
                             ignore_index=False, keys=None, levels=None, names=None)
        print(f"VIDEO COMMENTS EXTRACTED | PROCEEDING TO NEXT VIDEO | CURRENT COMMENT COUNT: {len(comments)}\n")
    print("EXTRACTION COMPLETE")
    return comments.reset_index()


# TRANSFORM FUNCTIONS
def clean_date(dataframe: pd.DataFrame) -> None:
    """
    Converts extracted datetime string into datetime format and replace it with only the date
    """
    dataframe['video_date'] = pd.to_datetime(dataframe['video_date']).dt.date
    dataframe['comment_date'] = pd.to_datetime(dataframe['comment_date']).dt.date


def clean_text(dataframe: pd.DataFrame) -> None:
    """
    Cleans the text column of the dataframe and adds a new column containing cleaned text
    that is lowercase, lemmatized, and with html entities and whitespaces removed.
    """
    # decode html entities
    dataframe['cleaned_text'] = dataframe['text'].apply(lambda row: unescape(row))

    # replace line breaks with spaces
    pattern = r'<br>'
    dataframe['cleaned_text'] = dataframe['cleaned_text'].apply(lambda row: re.sub(pattern, ' ', row))

    # remove hrefs but maintaining content inside tag
    pattern = r'<a\s+(?:[^>]*?\s+)?href="([^"]*)"[^>]*>(.*?)</a>'
    dataframe['cleaned_text'] = dataframe['cleaned_text'].apply(lambda row: re.sub(pattern, r'\2', row,
                                                                                   flags=re.IGNORECASE))

    # looping through patterns to remove usernames and any other special characters that may remain
    patterns = [r'(https?://)?(www\.)?(youtube\.com|youtu\.be)/[^\s]+',
                r'(@[A-Za-z0-9_]+)|([^A-Za-z0-9_, \t])|([^A-Za-z0-9  \t])|(\w+:\/\/\S+)|(UC[A-Za-z0-9]+)', r'<[^>]+>']
    for pattern in patterns:
        dataframe['cleaned_text'] = dataframe['cleaned_text'].apply(lambda row: re.sub(pattern, '', row,
                                                                                       flags=re.IGNORECASE))

    # lowercase
    dataframe['cleaned_text'] = dataframe['cleaned_text'].str.lower()

    # verbs
    dataframe['cleaned_text'] = dataframe['cleaned_text'].apply(lambda row: ' '.join([lemmatizer.lemmatize(x, 'v') for x in row.split()]))

    # adjectives
    dataframe['cleaned_text'] = dataframe['cleaned_text'].apply(lambda row: ' '.join([lemmatizer.lemmatize(x, 'a') for x in row.split()]))

    # nouns
    dataframe['cleaned_text'] = dataframe['cleaned_text'].apply(lambda row: ' '.join([lemmatizer.lemmatize(x, 'n') for x in row.split()]))

    # stopwords
    dataframe['cleaned_text'] = dataframe['cleaned_text'].apply(lambda row: ' '.join([word for word in row.split() if word not in stopwords]))

    # whitespace
    dataframe['cleaned_text'] = dataframe['cleaned_text'].str.replace(r'^\s+|\s+$', '')


def clean_text_sentiment_analysis(dataframe: pd.DataFrame) -> None:
    """
    A minimal version of the clean_text function that only removes html entities,
    usernames, and other special characters for use in sentiment analysis which
    takes into account capitalization and other contextual information.
    """
    # decode html entities
    dataframe['cleaned_text_sentiment'] = dataframe['text'].apply(lambda row: unescape(row))

    # replace line breaks with spaces
    pattern = r'<br>'
    dataframe['cleaned_text_sentiment'] = dataframe['cleaned_text_sentiment'].apply(lambda row: re.sub(pattern, ' ', row))

    # remove hrefs but maintaining content inside tag
    pattern = r'<a\s+(?:[^>]*?\s+)?href="([^"]*)"[^>]*>(.*?)</a>'
    dataframe['cleaned_text_sentiment'] = dataframe['cleaned_text_sentiment'].apply(lambda row: re.sub(pattern, r'\2', row,
                                                                                   flags=re.IGNORECASE))

    # looping through patterns to remove usernames and any other special characters that may remain
    patterns = [r'(https?://)?(www\.)?(youtube\.com|youtu\.be)/[^\s]+',
                r'(@[A-Za-z0-9_]+)|([^A-Za-z0-9_, \t])|([^A-Za-z0-9  \t])|(\w+:\/\/\S+)|(UC[A-Za-z0-9]+)', r'<[^>]+>']
    for pattern in patterns:
        dataframe['cleaned_text_sentiment'] = dataframe['cleaned_text_sentiment'].apply(
            lambda row: re.sub(pattern, '', row,flags=re.IGNORECASE))
