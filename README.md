<p align="center">
<img src="images/MusicFlow.png" width="600">
</p>

## Project Description
This project copies your music from YouTube or YouTube Music to Spotify. It can extract your music directly from YouTube using the [official API](https://developers.google.com/youtube/v3) or from YouTube Music using [ytmusicapi](https://github.com/sigma67/ytmusicapi). [Here](https://github.com/MishchenkoYuriy/MusicFlow#which-flow-to-choose) are the pros and cons of each.
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

<p align="center">
<img src="images/spotify.png" width="500">
</p>

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
- A <b>video title</b> may or may not include artists, track name, series/game, number in the album or custom dividers.
- An <b>album description</b> on YouTube usually includes track names, but the format may vary.
- A track or album on Spotify may have two or more videos on YouTube. These duplicate videos may present in the liked videos or in a same playlist.
- A video (track or album) can be saved in the different playlists.
- Playlists can have the same name.

## How does it find music?

Check out the [Tableau dashboard](https://public.tableau.com/app/profile/yuriy.mishchenko/viz/MusicFlow/MusicFlow) for a quick introduction.

Looking for a track, you enter just enough information to find it, but still you probably won't blindly save the first position in the search results. So we have a video, but how do we find the same track on Spotify? That's the problem this project was created for.

By providing a threshold (`THRESHOLD_MS` variable in the `.env` file) you tell to search videos with a duration less than the threshold as tracks, and those greater than or equal to the threshold as albums and playlists. For example, if you set `THRESHOLD_MS=720000`, a video that lasts for 11:59 will be recognised as a track. If no threshold is specified, all videos will be searched as tracks.

### Searching Tracks
1. `track:<fixed_title> artist:<author>` — first, we remove special symbols (e.g. brackets, `|`, `:`, `-`) and redundant words (e.g. year, `OST`) from titles. We use [Spotify syntax](https://support.spotify.com/us/article/search/) as the strictest search. Note that quotes aren't necessary.
2. `<fixed_title>` — the channel name may not be relevant (user-generated content).
3. `track "<fixed_title>"` — alternative to the Spotify colons. Trying to say that we want a track in a softer way.
4. `<author> <fixed_title>` — the author's name may not be exactly the same on Spotify, in which case we won't find the track on the first step.
5. `track "<title>"` — in some cases important information may have been deleted, such as a track version. If the title has been changed in any way, we search with the raw title.
6. `<title>`

Two conditions indicating that a track has been found:
1. Both the title and at least one of the artists are present in the video's title.
   - Caveat: On my data OSTs usually don't have artists in the titles. For them, matching title is enough.
2. The difference between the YouTube video and the track is less than or equal to 5 seconds.

### Searching Albums, EPs and Playlists
We look for the album first.

1. `<fixed_title>` — remove special symbols (e.g. brackets, `|`, `:`, `-`) and redundant words (e.g. year, `OST`, `Full Album`) from titles.
2. `<title>` — in some cases important information may have been deleted. If the title has been changed in any way, we search with the raw title.
3. `<author> <fixed_title>` — for other users' albums/playlists, we include the author in the search query.

If no album is found, we follow the same steps for the playlist.

Three conditions indicating that an album or playlist has been found:
1. The title on Spotify matches the title on YouTube, and the artist matches the channel name.
2. The difference between the YouTube video and the album/playlist is less than or equal to 40 seconds.
3. At least 60% of tracks found (the album/playlist must contain 4 or more tracks to give an objective percentage).
   - Why is the percentage so low? We probably want at least 90% of the tracks. My data had spelling mistakes, extra prefixes and suffixes, extra or missing characters (usually spaces or brackets), slightly different translations or transcriptions. For example, `Part 1` instead of `Pt. 1`. In these cases 60% is optimal to eliminate few tracks.

### Which flow to choose?
&nbsp; | Youtube Data API | ytmusicapi
--- | --- | ---
Support extraction of other users' albums, EPs and playlists | No | Yes
Amount of extracted music | Wide, some extracted videos may not be music | Strict, some music may not be extracted, especially uploaded videos
Your playlists | Extract all your playlists (you can manually exclude some of them) | Extract only playlists with at least one music video (no exclusion supported)

### Disclaimer
This project may be bad to copy:
- Classical music
- Clips, live performances, covers, remixes and extended versions

After the flow is finished, you can check what was found and what was not found by looking at the pre-made analyses in `dbt/analyses/` and views in `dbt/models/marts`. Read more about this in the dbt Docs.
