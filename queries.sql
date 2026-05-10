-- Top 10 most streamed songs
SELECT track, artist, spotify_streams
FROM spotify_songs
ORDER BY spotify_streams DESC
LIMIT 10;

-- Top 10 artists by total streams
SELECT artist, SUM(spotify_streams) AS total_streams
FROM spotify_songs
GROUP BY artist
ORDER BY total_streams DESC
LIMIT 10;

-- Number of songs released per year
SELECT release_year, COUNT(*) AS total_songs
FROM spotify_songs
WHERE release_year IS NOT NULL
GROUP BY release_year
ORDER BY release_year DESC;

-- Average streams by release year
SELECT release_year, ROUND(AVG(spotify_streams)) AS avg_streams
FROM spotify_songs
WHERE release_year IS NOT NULL
GROUP BY release_year
ORDER BY release_year DESC;

-- Most cross-platform songs (high on both Spotify and YouTube)
SELECT track, artist, spotify_streams, youtube_views
FROM spotify_songs
WHERE spotify_streams > 0 AND youtube_views > 0
ORDER BY (spotify_streams + youtube_views) DESC
LIMIT 10;