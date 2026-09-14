package co.wethinkcode.healthsafe;

import com.opencsv.CSVReader;
import io.javalin.Javalin;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class IngestionServiceApp {

    public static void main(String[] args) throws Exception {
        List<Map<String, Object>> cleanedWards = loadAndCleanWards();

        Javalin app = Javalin.create().start(7030);

        app.get("/health", ctx -> ctx.result("OK"));

        // Expose the cleaned records for ward-service to consume
        app.get("/wards", ctx -> ctx.json(cleanedWards));
    }

    private static List<Map<String, Object>> loadAndCleanWards() throws Exception {
        List<Map<String, Object>> results = new ArrayList<>();

        InputStream is = IngestionServiceApp.class.getResourceAsStream("/wards-outdated.csv");
        try (CSVReader reader = new CSVReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
            reader.readNext(); // skip header row
            String[] row;
            while ((row = reader.readNext()) != null) {
                results.add(cleanRow(row));
            }
        }

        return results;
    }

    private static Map<String, Object> cleanRow(String[] row) {
        Map<String, Object> record = new LinkedHashMap<>();

        String rawWardId = row.length > 0 ? row[0] : "";
        String rawWing = row.length > 1 ? row[1] : "";
        String rawDept = row.length > 2 ? row[2] : "";
        String rawBeds = row.length > 3 ? row[3] : "";

        record.put("wardId", rawWardId.trim().toUpperCase());
        record.put("wing", titleCaseAndCollapseSpaces(rawWing));
        record.put("department", titleCaseAndCollapseSpaces(rawDept));
        // bedsAvailable left as raw string for now — proper parsing/validation comes in the next stage
        record.put("bedsAvailable", rawBeds.trim());

        return record;
    }

    private static String titleCaseAndCollapseSpaces(String raw) {
        String collapsed = raw.trim().replaceAll("\\s+", " ");
        if (collapsed.isEmpty()) return "";
        StringBuilder sb = new StringBuilder();
        for (String word : collapsed.toLowerCase().split(" ")) {
            if (word.isEmpty()) continue;
            sb.append(Character.toUpperCase(word.charAt(0))).append(word.substring(1)).append(" ");
        }
        return sb.toString().trim();
    }
}