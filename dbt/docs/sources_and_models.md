{% docs playlists_ids %}

A mapping table between `youtube_playlists` and `spotify_playlists` by playlist ids (one-to-one relationship).

{% enddocs %}


{% docs search_types %}

This table contains search patterns that the engine uses to find albums, tracks and other users' playlists on Spotify.

{% enddocs %}


{% docs spotify_albums %}

This table contains your albums *found* on Spotify. The original album is a long video on YouTube.

{% enddocs %}


{% docs spotify_log %}

This table contains search details on your albums, tracks and other users' playlists *found* on Spotify and whether they were saved or skipped.<br>
Also can be described as a mapping table between YouTube videos and Spotify URIs.

{% enddocs %}


{% docs spotify_playlists %}

This table contains the Spotify playlists created during the flow.

{% enddocs %}


{% docs spotify_playlists_others %}

This table contains playlists created by other users *found* on Spotify. The original playlist is a long video on YouTube.

{% enddocs %}


{% docs spotify_tracks %}

This table contains your tracks *found* on Spotify, either by themselves or as a part of an album or a playlist created by other user.

{% enddocs %}


{% docs youtube_playlists %}

This table contains your playlists on YouTube (excluding `Liked videos`).

{% enddocs %}


{% docs youtube_videos %}

This table contains the videos presented in `Liked videos` or any of your playlists.

{% enddocs %}
