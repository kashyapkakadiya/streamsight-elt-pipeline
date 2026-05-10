-- This test PASSES if it returns 0 rows
-- It FAILS if any track has negative spotify streams

select
    track_name,
    artist_name,
    spotify_streams
from {{ ref('stg_spotify_songs') }}
where spotify_streams < 0