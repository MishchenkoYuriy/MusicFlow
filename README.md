<p align="center">
<img src="images/spotify.png" width="600">
</p>

## Project Description
This project copies your music from YouTube to Spotify.
YouTube | | Spotify
--- | --- | ---
Liked videos (tracks) | &rarr; | Liked songs
Liked videos (albums) | &rarr; | Saved albums in your Library
Your created playlists | &rarr; | Private, non-collaborative playlists

❕ The project does not transfer playlists created by other users and saved in your Library (YouTube Data API does not provide methods for working with this data).

## How to use it
Follow these [instructions](reproduce.md).

## Problem Statement
You may want to switch to Spotify (or at least back up your music data to Google BigQuery) for two reasons:
1. <b>Organise your music library more efficiently.</b><br>
I currently have 32 playlists and 550 liked music videos on YouTube, which adds up to 250 albums and 1000 tracks. From my experience of YouTube with [Enhancer](https://chrome.google.com/webstore/detail/enhancer-for-youtube/ponfpcnoihfmfllpaingbgckeeldkhle), at some point you need more options to structure your library. Creating a playlist on either YouTube Music or YouTube itself is slower than on Spotify. Spotify also wins when it comes to sorting and grouping.

2. <b>Protect your music from becoming unavailable.</b><br>
On YouTube anyone can upload their music and delete it just as easily. Today I have 80 deleted and 25 private videos, which is 10% of all my tracks. After a track is deleted or made private, you may not be able to retrieve any information to find what it was.
<br><br>
Some videos may not be available in your region (hidden songs in YouTube playlists or exclamation mark songs on YouTube Music), but you can still fetch them using the YouTube Data API and move to Spotify.


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
- Two or more videos of the same track or album can be liked.
- Two or more videos of the same track or album can be saved in one playlist.
- A video (track or album) can be saved in the different playlists.
- Playlists can have the same name.

## How does it find music? Exploring the Search Engine
The main problem is to find specific albums and tracks from the dirty YouTube videos. The first step to do that is to provide a threshold (`THRESHOLD_MS` variable in the `.env` file). The engine searches videos with a duration less than the threshold as tracks, and those greater than or equal to the threshold as albums. For example, if you set `THRESHOLD_MS=720000`, a video that lasts for 11:59 will be recognised as a track. If the treashold is not specified, the engine will search all videos as tracks.

### Searching tracks
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
The second try: `album "<video_title>"`

How does the search engine know an album has been found? It checks two conditions.
1. The difference between the YouTube video and the album is less than or equal to 40 seconds.
2. At least 70% of the album's tracks are present in the video's description (the album must contain 4 or more tracks to give an objective percentage).<br>
Why is the percentage so low? We probably want at least 90% of the tracks. My data had spelling mistakes, extra prefixes and suffixes, extra or missing characters (usually spaces or brackets), slightly different translations or transcriptions. For example, `Part 1` instead of `Pt. 1`. In these cases 70% is optimal to eliminate few tracks.

### Search disclaimer
This project may be bad to copy:
- Classical music
- Mixes and videos containing tracks from different creators
- Clips, live performances, covers, remixes and extended versions

After running `spotify_etl.py` you can check how well the search engine did by pre-made SQL queries in `analysis/`. Read more about it [here](analysis/README.md).