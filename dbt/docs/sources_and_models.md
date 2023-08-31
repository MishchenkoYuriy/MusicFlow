{% docs playlist_ids %}

A mapping table between `youtube_playlists` and `spotify_playlists` by playlist ids (one-to-one relationship).

{% enddocs %}


{% docs search_types %}

This table contains search patterns that the engine uses to find albums, tracks and other users' playlists on Spotify.

{% enddocs %}


{% docs spotify_albums %}

This table contains your albums *found* on Spotify. The original album is a long video on YouTube.

{% enddocs %}


{% docs spotify_log %}

This table contains search details on your albums, tracks and other users' playlists *found* on Spotify and whether they were saved or skipped.

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

{% docs log_found_videos %}

This model contains the found albums, playlists and tracks found on Spotify, the criteria used to find them, their status and the corresponding YouTube videos. This view can be used to analyse errors.

{% enddocs %}

{% docs log_not_found_videos %}

This model contains your YouTube videos not found on Spotify.

{% enddocs %}




{# COLUMNS #}



{% docs search_type_id %}

A unique identifier for the type of search.

{% enddocs %}

{% docs search_type_name %}

Spotify 

status | description
--- | ---
saved | An album, track or playlist that matches your video on YouTube has been saved to your library on Spotify.
skipped (saved before the run) | An album, track or playlist is already saved in your Spotify library before the run and therefore skipped. Since only playlists created during the run will be populated, this status will only apply to the liked entries. The main use: If you try to save a liked entry, its `added_at` attribute will be overwritten. Later, by using spotify_unlike scripts with the `remove_after` variable set, you may lose 'overliked' entries.
skipped (saved during the run) | An album, track or playlist is already saved in your Spotify library during the run and therefore skipped. It means that the found entiry is liked or in the current playlist.

{% enddocs %}
