import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.ZoneOffset;

public record StreamingHistoryEntry(LocalDateTime ts,
                                    long minutesPlayed,
                                    String trackName, String artistName, String albumName,
                                    String spotifyTrackUri) {

    // Record constructor with special parsing of ts and msPlayed
    public StreamingHistoryEntry(String ts,
                                 String msPlayed,
                                 String trackName, String artistName, String albumName,
                                 String spotifyTrackUri) {
        this(
                LocalDateTime.parse(ts, DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'").withZone(ZoneOffset.UTC)), // Parse the timestamp
                Long.parseLong(msPlayed) / 1000 / 60, // convert
                trackName, artistName, albumName,
                spotifyTrackUri
        );
    }
}