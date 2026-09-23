package co.wethinkcode.healthsafe;

import co.wethinkcode.healthsafe.mq.MqConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.javalin.Javalin;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class WardServiceApp {

    private static final String INGESTION_URL = "http://localhost:7030/wards";
    private static List<Map<String, Object>> wards;
    private static final Map<String, Object> latestStaffingUpdates = new ConcurrentHashMap<>();
    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        wards = fetchWardsFromIngestion();
        subscribeToStaffingEvents();

        Javalin app = Javalin.create().start(7031);

        app.get("/health", ctx -> ctx.result("OK"));

        app.get("/wards", ctx -> ctx.json(wards));

        app.get("/wards/{id}", ctx -> {
            String requestedId = ctx.pathParam("id");

            Map<String, Object> match = wards.stream()
                    .filter(w -> requestedId.equalsIgnoreCase((String) w.get("wardId")))
                    .findFirst()
                    .orElse(null);

            if (match == null) {
                ctx.status(404).json(Map.of("error", "Ward not found: " + requestedId));
                return;
            }

            ctx.json(match);
        });

        app.get("/wards/{id}/staffing", ctx -> {
            String requestedId = ctx.pathParam("id").toUpperCase();
            Object update = latestStaffingUpdates.get(requestedId);

            if (update == null) {
                ctx.status(404).json(Map.of("error", "No staffing update received yet for ward " + requestedId));
                return;
            }

            ctx.json(update);
        });
        app.post("/wards/{id}/equipment-failures", ctx -> {
            String requestedId = ctx.pathParam("id");

            Map<String, Object> ward = wards.stream()
                    .filter(w -> requestedId.equalsIgnoreCase((String) w.get("wardId")))
                    .findFirst()
                    .orElse(null);
            if (ward == null) {
                ctx.status(404).json(Map.of("error", "Ward not found: " + requestedId));
                return;
            }

            Map<String, Object> body = ctx.bodyAsClass(Map.class);
            Object equipment = body.get("equipment");
            if (!(equipment instanceof String) || ((String) equipment).isBlank()) {
                ctx.status(400).json(Map.of("error", "'equipment' is required"));
                return;
            }

            Map<String, Object> alert = new java.util.LinkedHashMap<>();
            alert.put("alertId", java.util.UUID.randomUUID().toString());
            alert.put("wardId", ward.get("wardId"));
            alert.put("equipment", ((String) equipment).trim());
            alert.put("detectedAt", java.time.Instant.now().toString());

            try {
                publishEquipmentFailure(alert);
            } catch (Exception e) {
                ctx.status(503).json(Map.of("error", "Alert could not be queued: " + e.getMessage()));
                return;
            }

            ctx.status(202).json(alert);
        });
    }

    private static List<Map<String, Object>> fetchWardsFromIngestion() throws Exception {
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(INGESTION_URL))
                .GET()
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        return mapper.readValue(response.body(), List.class);
    }

    private static void subscribeToStaffingEvents() throws JMSException {
        ConnectionFactory factory = new ActiveMQConnectionFactory(MqConfig.BROKER_URL);
        Connection connection = factory.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTopic(MqConfig.TOPIC);
        MessageConsumer consumer = session.createConsumer(topic);

        consumer.setMessageListener(message -> {
            try {
                if (message instanceof TextMessage textMessage) {
                    String json = textMessage.getText();
                    Map<String, Object> event = mapper.readValue(json, Map.class);
                    String wardId = ((String) event.get("wardId")).toUpperCase();

                    latestStaffingUpdates.put(wardId, event);
                    System.out.println("Received staffing event for " + wardId + ": " + json);
                }
            } catch (Exception e) {
                System.err.println("Error processing staffing event: " + e.getMessage());
            }
        });

        System.out.println("Subscribed to " + MqConfig.TOPIC + " — waiting for staffing events.");
    }
    private static void publishEquipmentFailure(Map<String, Object> alert) throws Exception {
        ConnectionFactory factory = new ActiveMQConnectionFactory(MqConfig.BROKER_URL);
        Connection connection = factory.createConnection();
        try {
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue(MqConfig.QUEUE);
            MessageProducer producer = session.createProducer(queue);
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);

            producer.send(session.createTextMessage(mapper.writeValueAsString(alert)));
        } finally {
            connection.close();
        }
    }
}