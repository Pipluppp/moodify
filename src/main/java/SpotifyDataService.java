import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.time.DayOfWeek;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SpotifyDataService {

    public List<StreamingHistoryEntry> parseCsv(InputStream inputStream) throws IOException {
        Reader reader = new InputStreamReader(inputStream);
        CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                .withFirstRecordAsHeader()
                .withIgnoreHeaderCase()
                .withTrim());

        List<StreamingHistoryEntry> entries = new ArrayList<>();
        for (CSVRecord record : csvParser) {
            StreamingHistoryEntry entry = new StreamingHistoryEntry(
                    record.get("ts"),
                    record.get("ms_played"),
                    record.get("master_metadata_track_name"),
                    record.get("master_metadata_album_artist_name"),
                    record.get("master_metadata_album_album_name"),
                    record.get("spotify_track_uri"),
                    record.get("reason_start"),
                    record.get("reason_end")
            );
            entries.add(entry);
        }
        return entries;
    }
    public List<String> getTopTrackUris(List<StreamingHistoryEntry> entries, int limit) {
        return entries.stream()
                .collect(Collectors.groupingBy(
                        StreamingHistoryEntry::getSpotifyTrackUri,
                        Collectors.summingLong(StreamingHistoryEntry::getMsPlayed)
                ))
                .entrySet().stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .limit(limit)
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
    }

    public Map<String, Long> getTopArtists(List<StreamingHistoryEntry> entries, int limit) {
        Map<String, Long> artistPlayCounts = entries.stream()
                .collect(Collectors.groupingBy(
                        StreamingHistoryEntry::getArtistName,
                        Collectors.summingLong(StreamingHistoryEntry::getMsPlayed)
                ));

        return artistPlayCounts.entrySet().stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .limit(limit)
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (e1, e2) -> e1,
                        LinkedHashMap::new
                ));
    }

    public Map<String, Long> getTopAlbums(List<StreamingHistoryEntry> entries, int limit) {
        Map<String, Long> albumPlayCounts = entries.stream()
                .collect(Collectors.groupingBy(
                        entry -> entry.getAlbumName() + " - " + entry.getArtistName(),
                        Collectors.summingLong(StreamingHistoryEntry::getMsPlayed)
                ));

        return albumPlayCounts.entrySet().stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .limit(limit)
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (e1, e2) -> e1,
                        LinkedHashMap::new
                ));
    }

    public long getTotalListeningTime(List<StreamingHistoryEntry> entries) {
        return entries.stream()
                .mapToLong(StreamingHistoryEntry::getMsPlayed)
                .sum();
    }

    public Map<DayOfWeek, Long> getMostListenedToDays(List<StreamingHistoryEntry> entries) {
        return entries.stream()
                .collect(Collectors.groupingBy(
                        entry -> entry.getTs().getDayOfWeek(),
                        Collectors.summingLong(StreamingHistoryEntry::getMsPlayed)
                ));
    }
}