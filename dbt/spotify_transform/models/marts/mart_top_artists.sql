with source as (
    select * from {{ ref('stg_spotify_songs') }}
),

artist_metrics as (
    select
        artist_name,

        -- track count
        count(*)                                    as total_tracks,

        -- spotify
        sum(spotify_streams)                        as total_spotify_streams,
        round(avg(spotify_streams))                 as avg_spotify_streams_per_track,
        max(spotify_streams)                        as max_single_track_streams,

        -- youtube
        sum(youtube_views)                          as total_youtube_views,
        sum(youtube_likes)                          as total_youtube_likes,

        -- tiktok
        sum(tiktok_views)                           as total_tiktok_views,
        sum(tiktok_posts)                           as total_tiktok_posts,

        -- cross-platform reach
        sum(spotify_streams + youtube_views + tiktok_views)  as total_cross_platform_reach,

        -- shazam
        sum(shazam_counts)                          as total_shazam_counts

    from source
    group by artist_name
),

ranked as (
    select
        *,
        rank() over (order by total_spotify_streams desc)       as spotify_rank,
        rank() over (order by total_cross_platform_reach desc)  as cross_platform_rank
    from artist_metrics
)

select * from ranked
order by spotify_rank