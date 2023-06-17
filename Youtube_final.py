import pandas as pd
import plotly.express as px
import streamlit as st
from streamlit_option_menu import option_menu
import mysql.connector as sql
import pymongo
from googleapiclient.discovery import build
from PIL import Image

# SETTING PAGE CONFIGURATIONS
image = Image.open('C:\\Users\\Admin\\Downloads\\Yt_api.PNG')
st.image(image)

# CREATING OPTION MENU
selected = option_menu(None, ["Home","Data migration","Outlook"], 
                           icons=["house-door-fill","tools","card-text"],
                           default_index=0,
                           orientation="horizontal",
                           styles={"nav-link": {"font-size": "30px", "text-align": "centre", "margin": "0px", 
                                                "--hover-color": "#c85701"},
                                   "icon": {"font-size": "30px"},
                                   "container" : {"max-width": "6000px"},
                                   "nav-link-selected": {"background-color": "#c85701"}})

# Bridging a connection with MongoDB Atlas and Creating a new database(youtube_data)
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client.youtube_data

# CONNECTING WITH MYSQL DATABASE
mydb = sql.connect(host="localhost",
                   user="root",
                   password="root",
                   database= "youtube_db"
                  )
mycursor = mydb.cursor(buffered=True)

# BUILDING CONNECTION WITH YOUTUBE API
api_key = "AIzaSyDW6cYDgdgJGJ6YVGwNWmeuWAmL1gnBlRE"
youtube = build('youtube','v3',developerKey=api_key)


# FUNCTION TO GET CHANNEL DETAILS
def get_channel_details(channel_id): #retrieves information about a YouTube channel based on its channel_id
    ch_data = [] #This line initializes an empty list.It will store the channel information.

    #This line makes a request to the YouTube API to retrieve information about the channel specified by channel_id. 
    #The part parameter indicates which parts of the channel information to include in the response.
    #execute the API request and store the response in the response variable.
    response = youtube.channels().list(part = 'snippet,contentDetails,statistics',
                                     id= channel_id).execute()

    for i in range(len(response['items'])): #loop that iterates over each item in the response['items']

        # It stores this information in a list of dictionaries, where each dictionary represents the details of a single channel. 
        # Finally, the function returns this list of channel details.
        data = dict(Channel_id = channel_id[i],
                    Channel_name = response['items'][i]['snippet']['title'],
                    Playlist_id = response['items'][i]['contentDetails']['relatedPlaylists']['uploads'],
                    Subscribers = response['items'][i]['statistics']['subscriberCount'],
                    Views = response['items'][i]['statistics']['viewCount'],
                    Total_videos = response['items'][i]['statistics']['videoCount'],
                    Description = response['items'][i]['snippet']['description'],
                    Country = response['items'][i]['snippet'].get('country')
                    )
        ch_data.append(data)
    return ch_data


# FUNCTION TO GET VIDEO IDS
def get_channel_videos(channel_id):
    video_ids = []
    # get Uploads playlist id
    #The execute() method is called to execute the API request, and the response is stored in the res variable.
    res = youtube.channels().list(id=channel_id, 
                                  part='contentDetails').execute()
    playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    #fetch the next page of results
    next_page_token = None 
    
    while True: #loop untill no more pages
        #pageToken parameter is used to fetch the next page
        res = youtube.playlistItems().list(playlistId=playlist_id, 
                                           part='snippet', 
                                           maxResults=50,
                                           pageToken=next_page_token).execute()

        #for loop iterates over the items in the API response and retrieves the video ID for each video, which is then appended to the video_ids list. 
        for i in range(len(res['items'])):
            video_ids.append(res['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = res.get('nextPageToken')
        
        if next_page_token is None:
            break
        #This line indicates the end of the loop. 
        #Once all the video IDs have been collected and stored in the video_ids list, the function returns the video_ids list as the result.
    return video_ids


# FUNCTION TO GET VIDEO DETAILS
def get_video_details(video_id):
    video_stats = [] #store the details of the videos
    
    for i in range(0, len(video_id), 50): #a limit on the number of video ID
        response = youtube.videos().list(
                    part="snippet,contentDetails,statistics",
                    id=','.join(video_id[i:i+50])).execute()
        for video in response['items']:
            #Each detail is assigned to a key in a dictionary called video_details
            #Within the inner loop, this block of code iterates over each video item in the API response and retrieves various details of the video
            #The details are stored in a list of dictionaries, where each dictionary represents the details of a single video.
            video_details = dict(Channel_name = video['snippet']['channelTitle'],
                                Channel_id = video['snippet']['channelId'],
                                Video_id = video['id'],
                                Title = video['snippet']['title'],
                                Tags = video['snippet'].get('tags'),
                                Thumbnail = video['snippet']['thumbnails']['default']['url'],
                                Description = video['snippet']['description'],
                                Published_date = video['snippet']['publishedAt'],
                                Duration = video['contentDetails']['duration'],
                                Views = video['statistics']['viewCount'],
                                Likes = video['statistics'].get('likeCount'),
                                Comments = video['statistics'].get('commentCount'),
                                Favorite_count = video['statistics']['favoriteCount'],
                                Definition = video['contentDetails']['definition'],
                                Caption_status = video['contentDetails']['caption']
                               )
            video_stats.append(video_details)
            #This line indicates the end of the loop
    return video_stats


# FUNCTION TO GET COMMENT DETAILS
def get_comments_details(v_id):
    comment_data = []
    try: #This begins a try-except block to handle any potential errors that may occur during the API requests.
        next_page_token = None
        #The following lines set up a while loop that continues until there are no more pages of comment results.
        while True:
            #This line makes a request to the YouTube API to retrieve the comment threads for the specified video ID
            response = youtube.commentThreads().list(part="snippet,replies",
                                                    videoId=v_id,
                                                    maxResults=100, #The maxResults parameter sets the maximum number of results per page to 100.
                                                    pageToken=next_page_token).execute()
            for cmt in response['items']:
                #Each detail is assigned to a key in a dictionary called data.
                #The data dictionary is then appended to the comment_data list.
                data = dict(Comment_id = cmt['id'],
                            Video_id = cmt['snippet']['videoId'],
                            Comment_text = cmt['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_author = cmt['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_posted_date = cmt['snippet']['topLevelComment']['snippet']['publishedAt'],
                            Like_count = cmt['snippet']['topLevelComment']['snippet']['likeCount'],
                            Reply_count = cmt['snippet']['totalReplyCount']
                           )
                comment_data.append(data)
            next_page_token = response.get('nextPageToken')
            if next_page_token is None:
                break
    except: #This block is used to catch any potential errors that may occur during the API requests
        pass # The pass statement is used to indicate that no action should be taken if an error occurs.
    return comment_data #which contains the details of the comments retrieved from the YouTube API.


# FUNCTION TO GET CHANNEL NAMES FROM MONGODB
def channel_names():   
    ch_name = []
    #This line sets up a loop that iterates over the documents in the MongoDB collection channel_details. 
    #It uses the find() method to retrieve all the documents in the collection.
    for i in db.channel_details.find():
        #Within the loop, this line retrieves the value of the Channel_name field from each document and appends it to the ch_name list.
        ch_name.append(i['Channel_name'])
        #This line indicates the end of the loop.Once all the channel names have been collected and stored in the ch_name list
        #the function returns the ch_name list as the result.
    return ch_name


# HOME PAGE
if selected == "Home":
    # Title Image
    st.subheader(":blue[Domain] : Social Media")
    st.subheader(":blue[Technologies used] : Python,MongoDB, Youtube Data API, MySql, Streamlit")
    st.subheader(":blue[Overview] : Retrieving the Youtube channels data from the Google API, storing it in a MongoDB as data lake, migrating and transforming data into a SQL database,then querying the data and displaying it in the Streamlit app.")
    
# EXTRACT AND TRANSFORM PAGE
# In this line checks if the selected option is "Data migration" and displays two tabs, one for extraction and one for transformation. 
if selected == "Data migration":
    tab1,tab2 = st.tabs(["$\huge EXTRACT $", "$\huge TRANSFORM $"])
    
    # EXTRACT TAB
    #This block of code is inside the first tab, which is for extraction.
    with tab1:
        st.markdown("#    ")
        st.write("### Enter YouTube Channel_ID below :")
        ch_id = st.text_input("Hint : Goto channel's home page > Right click > View page source > Find channel_id").split(',')

        #This code block is executed when the user enters channel IDs and clicks the "Extract Data" button
        if ch_id and st.button("Extract Data"):
            ch_details = get_channel_details(ch_id)
            st.write(f'#### Extracted data from :green["{ch_details[0]["Channel_name"]}"] channel')
            st.table(ch_details)

        #This code block is executed when the user clicks the "Upload to MongoDB" button.
        #The purpose of this code block is to extract the channel details, video details, and comment details 
        #based on the provided channel ID (ch_id) in order to prepare them for uploading to MongoDB.
        if st.button("Upload to MongoDB"):
            with st.spinner('Please Wait for it...'): #st.spinner() to indicate that the upload process is in progress.
                #ch_id parameter to retrieve the channel details,video IDs,video details.
                ch_details = get_channel_details(ch_id)
                video_id = get_channel_videos(ch_id)
                vid_details = get_video_details(video_id)
                
                def comments():
                    com_d = []
                    #iterates over the video IDs and calls the get_comments_details() function for each video ID to retrieve the comment details. 
                    for i in video_id:
                        com_d+= get_comments_details(i)
                    return com_d
                comm_details = comments()
                #The channel details are inserted into the channel_details collection
                collections1 = db.channel_details
                collections1.insert_many(ch_details)
                #the video details are inserted into the video_details collection
                collections2 = db.video_details
                collections2.insert_many(vid_details)
                #the comment details are inserted into the comments_details collection in MongoDB
                collections3 = db.comments_details
                collections3.insert_many(comm_details)
                st.success("Upload to MogoDB successful !!")
      
    # TRANSFORM TAB
    #This code is inside the second tab, which is for transformation. 
    #It displays a markdown header and a select box where the user can choose a channel to transform into SQL format.
    with tab2:     
        st.markdown("#   ")
        st.markdown("### Select a channel to begin Transformation to SQL")
        
        ch_names = channel_names()
        # The selected channel name is stored in the user_inp variable
        user_inp = st.selectbox("Select channel",options= ch_names)
        
        #These functions are defined to handle the insertion of data into MySQL tables.
        #The data is inserted using SQL queries executed with mycursor.execute() and committed to the database with mydb.commit()
        def insert_into_channels():
                collections = db.channel_details
                #if the SQL query has placeholders %s,%s,%s,%s, the values in the tuple will be assigned to these placeholders in the order they appear. 
                #The first value in the tuple will be assigned to the first %s placeholder, the second value to the second %s, and so on.
                query = """INSERT INTO channels VALUES(%s,%s,%s,%s,%s,%s,%s,%s)"""
                
                for i in collections.find({"Channel_name" : user_inp},{'_id' : 0}):
                    #By default, the returned tuple consists of data returned by the MySQL server, converted to Python objects. 
                    #If the cursor is a raw cursor, no such conversion occurs.
                    #The values are converted to a tuple using tuple(i.values()) because the execute() method of the MySQL cursor expects a tuple.
                    mycursor.execute(query,tuple(i.values()))
                    mydb.commit()
                
        def insert_into_videos():
            collections1 = db.video_details
            query1 = """INSERT INTO videos VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

            for i in collections1.find({"Channel_name" : user_inp},{'_id' : 0}):
                mycursor.execute(query1,tuple(i.values()))
                mydb.commit()

        def insert_into_comments():
            collections1 = db.video_details
            collections2 = db.comments_details
            query2 = """INSERT INTO comments VALUES(%s,%s,%s,%s,%s,%s,%s)"""

            for vid in collections1.find({"Channel_name" : user_inp},{'_id' : 0}):
                for i in collections2.find({'Video_id': vid['Video_id']},{'_id' : 0}):
                    mycursor.execute(query2,tuple(i.values()))
                    mydb.commit()

        if st.button("Submit"):
            try:
                #It calls the transformation functions (insert_into_channels(), insert_into_videos(), and insert_into_comments()) 
                #to insert the corresponding data into the MySQL tables.
                insert_into_channels()
                insert_into_videos()
                insert_into_comments()
                st.success("Transformation to MySQL Successful !!")
            except:
                st.error("Channel details already transformed !!")
            
# VIEW PAGE
if selected == "Outlook":
    
    st.write("## :orange[Select any question to get Insights]")
    questions = st.selectbox('Questions',
    ['1. What are the names of all the videos and their corresponding channels?',
    '2. Which channels have the most number of videos, and how many videos do they have?',
    '3. What are the top 10 most viewed videos and their respective channels?',
    '4. How many comments were made on each video, and what are their corresponding video names?',
    '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
    '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
    '7. What is the total number of views for each channel, and what are their corresponding channel names?',
    '8. What are the names of all the channels that have published videos in the year 2022?',
    '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
    '10. Which videos have the highest number of comments, and what are their corresponding channel names?'])
    
    if questions == '1. What are the names of all the videos and their corresponding channels?':
        mycursor.execute("""SELECT video_name AS Video_Title, channel_name AS Channel_Name
                            FROM video
                            ORDER BY channel_name""")
        #The fetchall() method retrieves all the rows returned by the SQL query.
        #The retrieved rows are used to create a Pandas DataFrame by passing the fetched data and column names (mycursor.column_names) to the pd.DataFrame() constructor.
        #the DataFrame df is displayed using st.write().
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        
    elif questions == '2. Which channels have the most number of videos, and how many videos do they have?':
        #The execute() method is called on the mycursor object to execute the SQL query.
        mycursor.execute("""SELECT channel_name AS Channel_Name, video_count AS Total_Videos
                            FROM channel
                            ORDER BY total_videos DESC""")
        #The fetchall() method retrieves all the rows returned by the SQL query.
        #The retrieved rows are used to create a Pandas DataFrame.
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        #which will show the channel names and their corresponding total video counts.
        st.write(df)
        st.write("### :green[Number of videos in each channel :]")
        
        fig = px.bar(df,
                     x=mycursor.column_names[0],
                     y=mycursor.column_names[1],
                     #The orientation parameter is set to 'v' to create a vertical bar chart.
                     orientation='v',
                     #The color parameter is set to mycursor.column_names[0] to color the bars based on the channel names.
                     color=mycursor.column_names[0]
                    )
        #The st.plotly_chart() function is used to render a Plotly chart in Streamlit.
        #By setting use_container_width=True, the chart will automatically adjust its width to fit the available space within the Streamlit app interface.
        #adjusting the width to fit the container.
        st.plotly_chart(fig,use_container_width=True)
        
    elif questions == '3. What are the top 10 most viewed videos and their respective channels?':
        mycursor.execute("""SELECT channel_name AS Channel_Name, video_name AS Video_Title, view_count AS Views 
                            FROM video
                            ORDER BY views DESC
                            LIMIT 10""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Top 10 most viewed videos :]")
        fig = px.bar(df,
                     x=mycursor.column_names[2],
                     y=mycursor.column_names[1],
                     orientation='h',
                     color=mycursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
        
    elif questions == '4. How many comments were made on each video, and what are their corresponding video names?':
        mycursor.execute("""SELECT channel.Channel_Name, video.Video_Name, video.Comment_Count FROM channel 
                            JOIN playlist ON channel.Channel_Id = playlist.Channel_Id JOIN video ON playlist.Playlist_Id = video.Playlist_Id""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
          
    elif questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name AS Channel_Name,video_name AS Title,like_count AS Likes_Count 
                            FROM video
                            ORDER BY like_count DESC
                            LIMIT 10""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Top 10 most liked videos :]")
        fig = px.bar(df,
                     x=mycursor.column_names[2],
                     y=mycursor.column_names[1],
                     orientation='h',
                     color=mycursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
        
    elif questions == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        mycursor.execute("""SELECT video_name AS Title, like_count AS Likes_Count
                            FROM video
                            ORDER BY like_count DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
         
    elif questions == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name AS Channel_Name, channel_views AS Views
                            FROM channel
                            ORDER BY channel_views DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Channels vs Views :]")
        fig = px.bar(df,
                     x=mycursor.column_names[0],
                     y=mycursor.column_names[1],
                     orientation='v',
                     color=mycursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
        
    elif questions == '8. What are the names of all the channels that have published videos in the year 2022?':
        mycursor.execute("""SELECT channel_name AS Channel_Name
                            FROM video
                            WHERE published_date LIKE '2022%'
                            GROUP BY channel_name
                            ORDER BY channel_name""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        
    elif questions == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel.Channel_Name, TIME_FORMAT(SEC_TO_TIME(AVG(TIME_TO_SEC(TIME(video.Duration)))), '%H:%i:%s') AS duration  FROM channel 
                        JOIN playlist ON channel.Channel_Id = playlist.Channel_Id 
                        JOIN video ON playlist.Playlist_Id = video.Playlist_Id GROUP by Channel_Name ORDER BY duration DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Avg video duration for channels :]")
        fig = px.bar(df,
                     x=mycursor.column_names[0],
                     y=mycursor.column_names[1],
                     orientation='v',
                     color=mycursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
        
    elif questions == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name AS Channel_Name,video_id AS Video_ID,comment_count AS Comments
                            FROM video
                            ORDER BY comment_count DESC
                            LIMIT 10""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Videos with most comments :]")
        fig = px.bar(df,
                     x=mycursor.column_names[1],
                     y=mycursor.column_names[2],
                     orientation='v',
                     color=mycursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
        
        
    
