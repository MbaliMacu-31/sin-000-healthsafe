package co.wethinkcode.healthsafe;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.javalin.Javalin;

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

            ctx.json(Map.of(
                    "ward", ward,
                    "alertLevel", alertLevel,
                    "doctorsOnCall", doctorsOnCall,
                    "doctors", generateDoctorList(doctorsOnCall)

            ));
        });
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


// MQ TODO: publishes to ActiveMQ topic MqConfig.TOPIC at MqConfig.BROKER_URL (see co.wethinkcode.healthsafe.mq.MqConfig)
