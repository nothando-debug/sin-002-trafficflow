package co.wethinkcode.trafficflow;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.javalin.Javalin;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
public class IntersectionServiceApp {

    public record Intersection(String id, String district, String signalType, Boolean active) {}

    private static final Map<String, Intersection> intersectionsDb = new ConcurrentHashMap<>();
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    public static void main(String[] args) {

        fetchIntersectionsFromIngestion();

        Javalin app = Javalin.create().start(7021);

        app.get("/health", ctx -> ctx.result("OK"));

        app.get("/intersections", ctx -> ctx.json(intersectionsDb.values()));

        app.get("/intersections/{id}", ctx -> {
            String id = ctx.pathParam("id").toUpperCase();
            Intersection record = intersectionsDb.get(id);

            if (record != null) {
                ctx.json(record);
            } else {
                ctx.status(404).result("Intersection not found: " + id);
            }
        });

    }

    private static void fetchIntersectionsFromIngestion() {
        try {
            HttpClient client = HttpClient.newHttpClient();
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:7020/intersections"))
                    .GET()
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 200) {
                List<Intersection> records = objectMapper.readValue(
                        response.body(),
                        new TypeReference<List<Intersection>>() {}
                );

                for (Intersection item : records) {
                    if (item.id() != null) {
                        intersectionsDb.put(item.id().toUpperCase(), item);
                    }
                }
                System.out.println("IntersectionService loaded " + intersectionsDb.size() + " canonical records from Ingestion.");
            } else {
                System.err.println("Failed to fetch intersections from Ingestion Service. HTTP status: " + response.statusCode());
            }
        } catch (Exception e) {
            System.err.println("Could not connect to Ingestion Service at http://localhost:7020/intersections: " + e.getMessage());
        }
    }
}

// MQ TODO: publishes a periodic heartbeat to ActiveMQ queue MqConfig.HEARTBEAT_QUEUE at
// MqConfig.BROKER_URL (see co.wethinkcode.trafficflow.mq.MqConfig), consumed by intersection-watchdog.
