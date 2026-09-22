package co.wethinkcode.trafficflow;

import io.javalin.Javalin;

import java.net.URI;
import java.net.http.*;
import java.util.concurrent.atomic.AtomicInteger;
import co.wethinkcode.trafficflow.mq.MqConfig;
import javax.jms.MessageConsumer;
import javax.jms.TextMessage;
import javax.jms.*;

import org.apache.activemq.ActiveMQConnectionFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
public class RoutingServiceApp {

    private static final AtomicInteger currentCongestionLevel = new AtomicInteger(0);
    private static final HttpClient httpClient = HttpClient.newHttpClient();
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) {
        startCongestionTopicListener();
        Javalin app = Javalin.create().start(7023);

        app.get("/health", ctx -> ctx.result("OK"));

        app.get("/route", ctx -> {
            String from = ctx.queryParam("from");
            String to = ctx.queryParam("to");

            if (from == null || to == null || from.isBlank() || to.isBlank()) {
                ctx.status(400).result("Missing required query parameters: 'from' and 'to'");
                return;
            }

            boolean fromValid = validateIntersection(from);
            boolean toValid = validateIntersection(to);

            if (!fromValid || !toValid) {
                ctx.status(404).result("One or both intersections not recognized: " + from + ", " + to);
                return;
            }

            int congestion = currentCongestionLevel.get();

            int estimatedTravelTimeMinutes = 10 + (congestion * 3);

            RouteResponse response = new RouteResponse(
                    from.toUpperCase(),
                    to.toUpperCase(),
                    congestion,
                    estimatedTravelTimeMinutes
            );

            ctx.json(response);
        });
    }

    private static boolean validateIntersection(String intersectionId) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:7021/intersections/" + intersectionId.trim().toUpperCase()))
                    .GET()
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            return response.statusCode() == 200;
        } catch (Exception e) {
            System.err.println("Failed to connect to IntersectionService: " + e.getMessage());
            return false;
        }
    }

    private static void startCongestionTopicListener() {
        Thread listenerThread = new Thread(() -> {
            try {
                ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(MqConfig.BROKER_URL);
                Connection connection = connectionFactory.createConnection();
                connection.start();

                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                Topic topic = session.createTopic(MqConfig.TOPIC);
                MessageConsumer consumer = session.createConsumer(topic);

                consumer.setMessageListener(message -> {
                    if (message instanceof TextMessage textMessage) {
                        try {
                            String json = textMessage.getText();
                            JsonNode node = objectMapper.readTree(json);
                            if (node.has("level")) {
                                int newLevel = node.get("level").asInt();
                                currentCongestionLevel.set(newLevel);
                                System.out.println("[RoutingService] Received MQ Congestion Update -> Level: " + newLevel);
                            }
                        } catch (Exception e) {
                            System.err.println("[RoutingService] Error parsing topic message: " + e.getMessage());
                        }
                    }
                });

                System.out.println("[RoutingService] Subscribed to ActiveMQ Topic: " + MqConfig.TOPIC);
            } catch (Exception e) {
                System.err.println("[RoutingService] Could not connect to ActiveMQ broker. (Will fallback to default congestion = 0). Error: " + e.getMessage());
            }
        });

        listenerThread.setDaemon(true);
        listenerThread.start();
    }

    public record RouteResponse(String from, String to, int currentCongestionLevel, int estimatedTravelTimeMinutes) {}
        // TODO (Provides estimated travel times based on congestion and intersection.)
        // Add domain endpoints for routing-service here.
    
}

// MQ TODO: subscribes to ActiveMQ topic MqConfig.TOPIC at MqConfig.BROKER_URL (see co.wethinkcode.trafficflow.mq.MqConfig)
