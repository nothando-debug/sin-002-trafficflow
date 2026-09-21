package co.wethinkcode.trafficflow;

import io.javalin.Javalin;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;
public class IngestionServiceApp {

    public record Intersection(String id, String district, String signalType, Boolean active) {}
    public static void main(String[] args) {
        List<Intersection> cleanedRecords = loadAndCleanCsv();
        Javalin app = Javalin.create().start(7020);

        app.get("/health", ctx -> ctx.result("OK"));
        app.get("/intersections", ctx -> ctx.json(cleanedRecords));
        // TODO: read and clean src/main/resources/intersections-legacy.csv (intersections, districts, signal types data —
        // trim whitespace, fix casing, normalize dates/booleans) and expose the
        // cleaned records here for the other services to consume.
    }

    private static List<Intersection> loadAndCleanCsv() {
        Map<String, Intersection> intersectionMap = new LinkedHashMap<>();

        try (InputStream is = IngestionServiceApp.class.getResourceAsStream("/intersections-legacy.csv")) {
            if (is == null) {
                System.err.println("Could not find intersections-legacy.csv in resources.");
                return Collections.emptyList();
            }

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
                String line;
                boolean isHeader = true;

                while ((line = reader.readLine()) != null) {
                    if (line.trim().isEmpty()) {
                        continue;
                    }

                    if (isHeader) {
                        isHeader = false;
                        continue;
                    }

                    String[] tokens = line.split(",", -1); // -1 keeps trailing empty strings
                    if (tokens.length < 4) {
                        continue;
                    }

                    String rawId = tokens[0];
                    String rawDistrict = tokens[1];
                    String rawSignalType = tokens[2];
                    String rawActive = tokens[3];

                    String cleanedId = cleanText(rawId);
                    if (cleanedId == null) {
                        continue; // Skip records without a valid ID
                    }
                    cleanedId = cleanedId.toUpperCase();

                    String cleanedDistrict = cleanText(rawDistrict);
                    if (cleanedDistrict != null) {
                        cleanedDistrict = capitalizeWord(cleanedDistrict);
                    }

                    String cleanedSignalType = cleanText(rawSignalType);
                    if (cleanedSignalType != null) {
                        cleanedSignalType = cleanedSignalType.toLowerCase();
                    }

                    Boolean cleanedActive = parseBoolean(rawActive);

                    Intersection record = new Intersection(cleanedId, cleanedDistrict, cleanedSignalType, cleanedActive);
                    intersectionMap.put(cleanedId, record);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return new ArrayList<>(intersectionMap.values());
    }

    private static String cleanText(String raw) {
        if (raw == null) return null;

        String trimmed = raw.trim().replaceAll("\\s+", " ");
        if (trimmed.isEmpty()) return null;

        String lower = trimmed.toLowerCase();
        Set<String> placeholders = Set.of("n/a", "tbd", "unknown", "-", "nan", "null");
        if (placeholders.contains(lower)) {
            return null;
        }

        return trimmed;
    }

    private static String capitalizeWord(String text) {
        if (text == null || text.isEmpty()) return text;
        return Character.toUpperCase(text.charAt(0)) + text.substring(1);
    }

    private static Boolean parseBoolean(String raw) {
        String cleaned = cleanText(raw);
        if (cleaned == null) return false;

        String lower = cleaned.toLowerCase();
        return lower.equals("y") || lower.equals("yes") || lower.equals("1") || lower.equals("true");
    }


}
