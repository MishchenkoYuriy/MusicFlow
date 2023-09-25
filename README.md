<p align="center">
<img src="images/spotify.png" width="600">
</p>

## Project Description
This project copies your music from YouTube to Spotify. It can extract your music directly from YouTube using the [official API](https://developers.google.com/youtube/v3) or from YouTube Music using [ytmusicapi](https://github.com/sigma67/ytmusicapi).
YouTube | | Spotify
--- | --- | ---
Tracks in Liked videos | &rarr; | 💚 Liked songs
Albums in Liked videos | &rarr; | 💚 Albums in your Library
Playlists in Liked videos | &rarr; | 💚 Playlists in your Library
Your playlists | &rarr; | Private¹, non-collaborative playlists
Other users' albums, EPs and playlists² | &rarr; | 💚 Albums, EPs and playlists in your Library

¹ There are three types of Spotify playlists: public (listed on your profile), public (not listed on your profile, but accessible via link) and private (not listed on your profile, not accessible via link). The Spotify API only provides options to create the first and second types. Once your playlists have been created you will see 'Public Playlist' above the playlist title, which means the second option. For more information, read this [thread](https://community.spotify.com/t5/Spotify-for-Developers/Api-to-create-a-private-playlist-doesn-t-work/td-p/5407807).<br>
You can manually make a playlist private by following these steps: Open the playlist, click the three dots icon (...) at the top of the playlist (or right-click the playlist), and select `Make private`.

² For the ytmusicapi only. The YouTube Data API does not provide methods to work with this data.

## How to use it
Follow these [instructions](reproduce.md).

## Problem Statement
You may want to switch to Spotify (or at least back up your music) for two reasons:
1. <b>Organise your music library more efficiently.</b><br>
I currently have 32 playlists and 550 liked music videos on YouTube, which adds up to 250 albums and 1000 tracks. From my experience of YouTube with [Enhancer](https://chrome.google.com/webstore/detail/enhancer-for-youtube/ponfpcnoihfmfllpaingbgckeeldkhle), at some point you need more options to structure your library. Creating a playlist on either YouTube Music or YouTube itself is slower than on Spotify. Spotify also wins when it comes to sorting and grouping.

2. <b>Protect your music from becoming unavailable.</b><br>
On YouTube anyone can upload their music and delete it just as easily. Today I have 80 deleted and 25 private videos, which is 10% of all my tracks. After a track is deleted or made private, you may not be able to retrieve any information to find what it was.<br>
Some videos may not be available in your region (hidden songs in YouTube playlists or exclamation mark songs on YouTube Music), but you can still fetch them using the YouTube Data API and copy to Spotify.

### Why Spotify?
I found these Spotify features useful for organising my music:
- Nested folders
- Drag and drop to create a playlist, add or remove a track from the playlist
- Pinning folders and playlists
- Custom sorting
- Compact library layout
- Play Queue
- Much more...

## About dataset
This project uses channel names, video titles, descriptions and durations from YouTube. When working with this data, you should be aware of the following issues:
- A video can be uploaded by any user, so the <b>channel name</b> may not be relevant.
- A <b>track duration</b> may vary slightly between music platforms.
- A <b>video title</b> may or may not include artists, track name or series/game.
- An <b>album description</b> on YouTube usually includes track names, but the format may vary.
- A track or album on Spotify may have two or more videos on YouTube. These duplicate videos may present in the liked videos or in a same playlist.
- A video (track or album) can be saved in the different playlists.
- Playlists can have the same name.

## How does it find music? Exploring the Search Engine
Let's say you're looking for a track, you probably won't blindly save the first position in your search. So we have a video, but how do we find the same track on Spotify? This is the main problem with this project.

The first step in finding albums, playlists and tracks from the dirty YouTube videos is to provide a threshold (`THRESHOLD_MS` variable in the `.env` file). The engine searches videos with a duration less than the threshold as tracks, and those greater than or equal to the threshold as albums and playlists. For example, if you set `THRESHOLD_MS=720000`, a video that lasts for 11:59 will be recognised as a track. If the threshold is not specified, the engine will search all videos as tracks.

### Things to consider
- The `limit` affects the result and doesn't just concatenate it
- Special characters in titles

### Searching Tracks
1. If the video belongs to a [topic channel](https://support.google.com/youtube/answer/7636475?hl=en#zippy=%2Chow-does-youtube-decide-when-to-auto-generate-a-topic-channel-for-an-artist) (` - Topic` in the channel name), the title is probably a track name and the channel name (without ` - Topic`) is the name of an artist. The engine will use Spotify syntax as the strictest search:<br>
`track:<video_title> artist:<channel_name>`
2. If it's not a topic channel video, the engine will <b>instead</b> search for the video title:<br>
`<video_title>`
3. In some cases (~8% on my data) the engine may not be able to find the track from the the topic channel video.<br>
`track "<video_title>"`
4. As a final option, the engine will concatenate the channel name and the video title.<br>
`<channel_name> <video_title>`

How does the search engine know a track has been found? It checks two conditions.
1. The difference between the YouTube video and the track is less than or equal to 5 seconds.
2. Both the title of the track and at least one of the artists are present in the video's title.

### Searching Albums
The first try: `<video_title>`<br>
The second try: `album "<video_title>"`<br>
If an album is not found, the engine will look for a playlist.

How does the search engine know an album has been found? It checks two conditions.
1. The difference between the YouTube video and the album is less than or equal to 40 seconds.
2. At least 70% of the album's tracks are present in the video's description (the album must contain 4 or more tracks to give an objective percentage).<br>
Why is the percentage so low? We probably want at least 90% of the tracks. My data had spelling mistakes, extra prefixes and suffixes, extra or missing characters (usually spaces or brackets), slightly different translations or transcriptions. For example, `Part 1` instead of `Pt. 1`. In these cases 70% is optimal to eliminate few tracks.

### Searching Playlists
TODO

### Which flow to choose?
&nbsp; | Youtube Data API | ytmusicapi
--- | --- | ---
Support extraction of other users' albums, EPs and playlists | No | Yes
Amount of extracted music | Wide, some extracted videos may not be music | Strict, some music may not be extracted, especially uploaded videos

### Disclaimer
This project may be bad to copy:
- Classical music
- Clips, live performances, covers, remixes and extended versions

When the flow is finished, you can check how well the search engine has done by looking at the pre-made analyses in `dbt/analyses/` and views in `dbt/models/marts`. Read more about this in the dbt Docs.