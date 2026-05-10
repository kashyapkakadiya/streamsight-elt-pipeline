with source as (
    select * from public.spotify_songs
),

renamed as (
    select
        -- identifiers
        track                           as track_name,
        artist                          as artist_name,

        -- release info
        release_date                    as release_date,
        release_year                    as release_year,

        -- spotify metrics
        spotify_streams                 as spotify_streams,
        spotify_playlist_count          as spotify_playlist_count,
        spotify_playlist_reach          as spotify_playlist_reach,

        -- youtube metrics
        youtube_views                   as youtube_views,
        youtube_likes                   as youtube_likes,

        -- tiktok metrics
        tiktok_posts                    as tiktok_posts,
        tiktok_likes                    as tiktok_likes,
        tiktok_views                    as tiktok_views,

        -- other platforms
        apple_music_playlist_count      as apple_music_playlists,
        deezer_playlist_count           as deezer_playlists,
        amazon_playlist_count           as amazon_playlists,
        pandora_streams                 as pandora_streams,
        soundcloud_streams              as soundcloud_streams,
        shazam_counts                   as shazam_counts

    from source
    where track is not null
      and artist is not null
      and spotify_streams > 0
)

select * from renamed