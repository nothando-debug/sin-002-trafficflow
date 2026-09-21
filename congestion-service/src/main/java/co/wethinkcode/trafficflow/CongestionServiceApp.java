package co.wethinkcode.trafficflow;

import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.DeliveryMode;
import javax.jms.MessageProducer;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.activemq.ActiveMQConnectionFactory;

import io.javalin.Javalin;
import io.javalin.http.BadRequestResponse;

public class CongestionServiceApp {

    private static final AtomicInteger congestionLevel = new AtomicInteger(0);
    public static void main(String[] args) {
        Javalin app = Javalin.create().start(7022);

        app.get("/health", ctx -> ctx.result("OK"));


        app.get("/congestion", ctx -> {
            ctx.json(new CongestionResponse(congestionLevel.get()));
        });


        app.post("/congestion", ctx -> {
            CongestionRequest request = ctx.bodyAsClass(CongestionRequest.class);
            int newLevel = request.level();

            if (newLevel < 0 || newLevel > 8) {
                throw new BadRequestResponse("Congestion level must be between 0 and 8.");
            }

            congestionLevel.set(newLevel);

            publishCongestionUpdate(newLevel);

            ctx.status(200).json(new CongestionResponse(newLevel));
        });
        // TODO (Tracks the city-wide Congestion Level (0-8).)
        // Add domain endpoints for congestion-service here.
    }

    private static void publishCongestionUpdate(int level) {
        try {
            ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(MqConfig.BROKER_URL);
            Connection connection = connectionFactory.createConnection();
            connection.start();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic topic = session.createTopic(MqConfig.TOPIC);
            MessageProducer producer = session.createProducer(topic);
            producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

            String payload = String.format("{\"level\": %d}", level);
            TextMessage message = session.createTextMessage(payload);

            producer.send(message);

            // Clean up resources
            producer.close();
            session.close();
            connection.close();
        } catch (Exception e) {
            System.err.println("Failed to publish message to ActiveMQ: " + e.getMessage());
        }



    


}

// MQ TODO: publishes to ActiveMQ topic MqConfig.TOPIC at MqConfig.BROKER_URL (see co.wethinkcode.trafficflow.mq.MqConfig)
