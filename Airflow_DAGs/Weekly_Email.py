#This is the code for the weekly Spotify Wrap Up Email
import psycopg2
import smtplib,ssl
import json
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from tabulate import tabulate
from datetime import datetime, timedelta

def weekly_email_function():
    conn = psycopg2.connect(host = "localhost",port="5432", dbname = "SpotifyDB",password="password",user="postgres",options="-c search_path=spotify_schema")
    cur = conn.cursor()
    today = datetime.today().date()
    six_days_ago = today - timedelta(days=6)

    #Top 5 Songs by Time Listened (MIN)
    top_5_songs_min = [['Song Name', 'Time (Min)']]
    cur.execute("""
    SELECT   st.song_name, 
         Round(Sum(Cast(duration_ms AS DECIMAL)/60000),2) AS min_duration 
        FROM     spotify_track                                    AS st 
        WHERE    date_time_played > CURRENT_DATE - interval '7 days' 
        GROUP BY st.song_name 
        ORDER BY min_duration DESC limit 5;
    """)
    for row in cur.fetchall():
        song_name = row[0]
        min_listened = float(row[1])
        element = [song_name,min_listened]
        top_5_songs_min.append(element)

    #Total Time Listened (HOURS)
    cur.execute("""
    SELECT ROUND(SUM(CAST (duration_ms AS decimal)/3600000),2) AS total_time_listened_hrs
    FROM spotify_track
    WHERE date_time_played > CURRENT_DATE - INTERVAL '7 days';
    """)
    total_time_listened_hrs = float(cur.fetchone()[0])

    #Top 5 Songs and Artists by Times Played
    top_songs_art_played = [['Song Name','Arist Name','Times Played']]
    cur.execute("""
    SELECT st.song_name, sa.name AS artist_name,COUNT(st.*) AS times_played
    FROM spotify_track AS st
    INNER JOIN spotify_artists AS sa 
    ON st.artist_id = sa.artist_id
    WHERE date_time_played > CURRENT_DATE - INTERVAL '7 days'
    GROUP BY st.song_name, sa.name
    ORDER BY times_played DESC
    LIMIT 5;
    """)
    for row in cur.fetchall():
        song_name = row[0]
        artist_name = row[1]
        times_played = int(row[2])
        element = [song_name,artist_name,times_played]
        top_songs_art_played.append(element)

    #Top Artists Played
    top_art_played = [['Artist Name','Times Played']]
    cur.execute("""SELECT art.name, COUNT(track.*):: INT AS number_plays
    FROM spotify_schema.spotify_track AS track
    INNER JOIN spotify_schema.spotify_artists AS art ON track.artist_id=art.artist_id
    WHERE date_time_played > CURRENT_DATE - INTERVAL '7 days'
    GROUP BY art.name
    ORDER BY number_plays DESC
    LIMIT 5;""")
    for row in cur.fetchall():
        artist_name = row[0]
        times_played = int(row[1])
        element = [artist_name,times_played]
        top_art_played.append(element)

    cur.execute("""CREATE OR REPLACE VIEW track_decades AS
        SELECT *,
        CASE 
        WHEN subqry.release_year >= 1950 AND subqry.release_year <= 1959  THEN '1950''s'
        WHEN subqry.release_year >= 1960 AND subqry.release_year <= 1969  THEN '1960''s'
        WHEN subqry.release_year >= 1970 AND subqry.release_year <= 1979  THEN '1970''s'
        WHEN subqry.release_year >= 1980 AND subqry.release_year <= 1989  THEN '1980''s'
        WHEN subqry.release_year >= 1990 AND subqry.release_year <= 1999  THEN '1990''s'
        WHEN subqry.release_year >= 2000 AND subqry.release_year <= 2009  THEN '2000''s'
        WHEN subqry.release_year >= 2010 AND subqry.release_year <= 2019  THEN '2010''s'
        WHEN subqry.release_year >= 2020 AND subqry.release_year <= 2029  THEN '2020''s'
        WHEN subqry.release_year >= 2030 AND subqry.release_year <= 2039  THEN '2030''s'
        WHEN subqry.release_year >= 2040 AND subqry.release_year <= 2049  THEN '2040''s'
        ELSE 'Other'
        END AS decade
        FROM 
        (SELECT album.album_id,album.name,album.release_date,track.unique_identifier,track.date_time_played,track.song_name,CAST(SPLIT_PART(release_date,'-',1) AS INT) AS release_year
        FROM spotify_album AS album
        INNER JOIN spotify_track AS track ON track.album_id = album.album_id) AS subqry;""")
    #Top Decades:
    top_decade_played = [['Decade','Times Played']]
    cur.execute("""SELECT decade, COUNT(unique_identifier) AS total_plays
        FROM track_decades
        WHERE date_time_played > CURRENT_DATE - INTERVAL '7 days'
        GROUP BY decade
        ORDER BY total_plays DESC;""")
    for row in cur.fetchall():
        decade = row[0]
        times_played = int(row[1])
        element = [decade,times_played]
        top_decade_played.append(element)

    #Sending the Email:
    port = 465
    password = "gmail_password"

    sender_email = "sender_email_address"
    receiver_email = "receiver_email_address"

    message = MIMEMultipart("alternative")
    message["Subject"] = f"Spotify - Weekly Roundup - {today}"
    message["From"] = sender_email
    message["To"] = receiver_email

    text = f"""\
    Here are your stats for your weekly round up for Spotify. 
    Dates included: {six_days_ago} - {today}:
    
    Total Time Listened: {total_time_listened_hrs} hours.
    You listened to these songs and artists a lot here are your top 5!
    {top_songs_art_played}
    You spent the most time listening to these songs:
    {top_5_songs_min}
    You spend the most time listening to these artists:
    {top_art_played}
    Lastly your top decades are as follows:
    {top_decade_played}
    """
    html = f"""\
    <html>
        <body>
            <h4>
            Here are your stats for your weekly round up for Spotify.
            </h4>
            <p>
            Dates included: {six_days_ago} - {today}
            <br>
            Total Time Listened: {total_time_listened_hrs} hours.
            <br>
            <h4>
            You listened to these songs and artists a lot here are your top 5!
            </h4>
            {tabulate(top_songs_art_played, tablefmt='html')}
            <h4>
            You spend a lot of time listening to these songs!
            </h4>
            {tabulate(top_5_songs_min, tablefmt='html')}
            <h4>
            You spend a lot of time listening to these artists!
            </h4>
            {tabulate(top_art_played, tablefmt='html')}
            <h4>
            Lastly your top decades are as follows:
            </h4>
            {tabulate(top_decade_played, tablefmt='html')}
            </p>
        </body>
    </html>"""

    part1 = MIMEText(text,"plain")
    part2 = MIMEText(html,"html")

    message.attach(part1)
    message.attach(part2)

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com",port,context = context) as server:
        server.login(sender_email,password)
        server.sendmail(sender_email,receiver_email,message.as_string())
    return "Email Sent"