with source as (
    select * from {{ ref('stg_spotify_songs') }}
),

yearly_metrics as (
    select
        release_year,

        -- volume
        count(*)                                        as total_tracks_released,
        count(distinct artist_name)                     as unique_artists,

        -- spotify
        sum(spotify_streams)                            as total_spotify_streams,
        round(avg(spotify_streams))                     as avg_spotify_streams_per_track,
        max(spotify_streams)                            as peak_track_streams,

        -- youtube
        sum(youtube_views)                              as total_youtube_views,
        round(avg(youtube_views))                       as avg_youtube_views_per_track,

        -- tiktok
        sum(tiktok_views)                               as total_tiktok_views,
        sum(tiktok_posts)                               as total_tiktok_posts,

        -- cross-platform
        sum(spotify_streams + youtube_views + tiktok_views)   as total_cross_platform_reach,

        -- shazam
        sum(shazam_counts)                              as total_shazam_counts

    from source
    where release_year is not null
    group by release_year
),

with_growth as (
    select
        *,

        -- year-over-year stream growth
        lag(total_spotify_streams) over (order by release_year)   as prev_year_streams,

        round(
            100.0 * (total_spotify_streams - lag(total_spotify_streams)
            over (order by release_year))
            / nullif(lag(total_spotify_streams) over (order by release_year), 0)
        , 2)                                                       as yoy_stream_growth_pct,

        -- rank years by total streams
        rank() over (order by total_spotify_streams desc)         as streams_rank

    from yearly_metrics
)

select * from with_growth
order by release_year desc