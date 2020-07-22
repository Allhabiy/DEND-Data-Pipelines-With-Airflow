class SqlQueries:
    songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id,artist_id, session_id, location, user_agent)
    SELECT timestamp 'epoch' + CAST(e.ts AS BIGINT)/1000 * interval '1 second', e.userId, e.level, s.song_id, s.artist_id,e.sessionId, e.location, e.userAgent
        FROM staging_events e
        LEFT JOIN staging_songs s ON e.artist = s.artist_name AND e.song = s.title AND e.length = s.duration WHERE e.page = 'NextSong';
""")

    user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT userId, firstName, lastName, gender, level
        FROM staging_events
        WHERE userId IS NOT NULL;
""")

    song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id, title, artist_id, year, duration
        FROM staging_songs
        WHERE song_id IS NOT NULL;
""")

    artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
        WHERE artist_id IS NOT NULL;
""")

    time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT start_time,
           EXTRACT(hour FROM start_time),
           EXTRACT(day FROM start_time),
           EXTRACT(week FROM start_time),
           EXTRACT(month FROM start_time),
           EXTRACT(year FROM start_time),
           EXTRACT(weekday FROM start_time)
        FROM songplays
        WHERE start_time IS NOT NULL;
""")