{% docs youtube_statistics %}

This analysis counts the number of albums, tracks and other users' playlists from YouTube videos by section, such as `Liked videos` or your playlists.

If in your case these sections contain a certain percent of non-music, add a coefficient to the `total_records` to get a more accurate number.

For example, `round(count(video_id)*0.6) as total_reconds` for 60% music videos.

{% enddocs %}

{% docs videos_saved_more_than_once %}

This analysis contains YouTube videos that are saved in multiple places, such as `Liked videos` and your playlists.

{% enddocs %}

{% docs most_saved_channels %}

This analysis shows the YouTube channels and the number of their videos you have saved in `Liked videos` or your playlists.

{% enddocs %}
