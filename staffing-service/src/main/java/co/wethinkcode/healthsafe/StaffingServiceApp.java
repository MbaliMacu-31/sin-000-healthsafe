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

public class StaffingServiceApp {

    private static final String WARD_SERVICE_URL = "http://localhost:7031/wards/";
    private static final String ALERT_LEVEL_URL = "http://localhost:7032/alert-level";
    private static final HttpClient client = HttpClient.newHttpClient();
    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) {
        Javalin app = Javalin.create().start(7033);

        app.get("/health", ctx -> ctx.result("OK"));

        app.get("/schedule/{wardId}", ctx -> {
            String wardId = ctx.pathParam("wardId");

            Map<String, Object> ward;
            try {
                ward = fetchWard(wardId);
            } catch (WardNotFoundException e) {
                ctx.status(404).json(Map.of("error", "Cannot schedule: " + e.getMessage()));
                return;
            } catch (Exception e) {
                ctx.status(502).json(Map.of("error", "ward-service unavailable: " + e.getMessage()));
                return;
            }

            int alertLevel;
            try {
                alertLevel = fetchAlertLevel();
            } catch (Exception e) {
                ctx.status(502).json(Map.of("error", "alert-level-service unavailable: " + e.getMessage()));
                return;
            }

            int doctorsOnCall = computeDoctorsOnCall(alertLevel);

            // Stage 3: broadcast this schedule/status change asynchronously.
            // A broker hiccup here should never break the REST response the caller is waiting on,
            // so failures are logged, not thrown.
            publishStaffingEvent(wardId, alertLevel, doctorsOnCall);

            ctx.json(Map.of(
                    "ward", ward,
                    "alertLevel", alertLevel,
                    "doctorsOnCall", doctorsOnCall,
                    "doctors", generateDoctorList(doctorsOnCall)
            ));
        });
    }

    private static void publishStaffingEvent(String wardId, int alertLevel, int doctorsOnCall) {
        try {
            ConnectionFactory factory = new ActiveMQConnectionFactory(MqConfig.BROKER_URL);
            try (Connection connection = factory.createConnection()) {
                connection.start();
                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                Topic topic = session.createTopic(MqConfig.TOPIC);
                MessageProducer producer = session.createProducer(topic);

                String json = String.format(
                        "{\"wardId\":\"%s\",\"alertLevel\":%d,\"doctorsOnCall\":%d}",
                        wardId, alertLevel, doctorsOnCall
                );
                TextMessage message = session.createTextMessage(json);
                producer.send(message);
                System.out.println("Published staffing event: " + json);
            }
        } catch (JMSException e) {
            System.err.println("Failed to publish staffing event (broker may be down): " + e.getMessage());
        }
    }

    private static Map<String, Object> fetchWard(String wardId) throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(WARD_SERVICE_URL + wardId))
                .GET()
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() == 404) {
            throw new WardNotFoundException("ward " + wardId + " does not exist");
        }
        if (response.statusCode() != 200) {
            throw new RuntimeException("unexpected status " + response.statusCode());
        }

        return mapper.readValue(response.body(), Map.class);
    }

    private static int fetchAlertLevel() throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(ALERT_LEVEL_URL))
                .GET()
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() != 200) {
            throw new RuntimeException("unexpected status " + response.statusCode());
        }

        Map<String, Object> body = mapper.readValue(response.body(), Map.class);
        return (Integer) body.get("level");
    }

    private static int computeDoctorsOnCall(int alertLevel) {
        return 1 + alertLevel;
    }

    private static List<String> generateDoctorList(int count) {
        return java.util.stream.IntStream.rangeClosed(1, count)
                .mapToObj(i -> "On-call Doctor " + i)
                .toList();
    }

    private static class WardNotFoundException extends Exception {
        WardNotFoundException(String message) {
            super(message);
        }
    }
}
