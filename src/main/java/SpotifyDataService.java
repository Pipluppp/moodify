import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

import java.util.ArrayList;
import java.util.List;

public class SpotifyDataService {
    public static List<StreamingHistoryEntry> parseCsv(InputStream inputStream) throws IOException {
        Reader reader = new InputStreamReader(inputStream);
        CSVParser csvParser = CSVParser.parse(reader, CSVFormat.DEFAULT.builder()
                .setHeader()
                .setSkipHeaderRecord(true)
                .setIgnoreHeaderCase(true)
                .setTrim(true)
                .build());

        List<StreamingHistoryEntry> entries = new ArrayList<>();
        for (CSVRecord record : csvParser) {
            StreamingHistoryEntry entry = new StreamingHistoryEntry(
                    record.get("ts"),
                    record.get("ms_played"),
                    record.get("master_metadata_track_name"),
                    record.get("master_metadata_album_artist_name"),
                    record.get("master_metadata_album_album_name"),
                    record.get("spotify_track_uri")
            );
            entries.add(entry);
        }
        return entries;
    }

    public static Object analyzeData(List<StreamingHistoryEntry> entries, Analysis analysis, SpotifyAPIService api) {
        return analysis.analyze(entries, api);
    }
}