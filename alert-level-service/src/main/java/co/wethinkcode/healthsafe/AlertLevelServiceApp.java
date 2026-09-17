package co.wethinkcode.healthsafe;

import io.javalin.Javalin;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class AlertLevelServiceApp {

    private static final AtomicInteger currentLevel = new AtomicInteger(0);

    public static void main(String[] args) {
        Javalin app = Javalin.create().start(7032);

        app.get("/health", ctx -> ctx.result("OK"));

        app.get("/alert-level", ctx -> ctx.json(Map.of("level", currentLevel.get())));

        app.post("/alert-level", ctx -> {
            Map<String, Object> body = ctx.bodyAsClass(Map.class);
            Object rawLevel = body.get("level");

            if (!(rawLevel instanceof Integer)) {
                ctx.status(400).json(Map.of("error", "level must be an integer"));
                return;
            }

            int newLevel = (Integer) rawLevel;
            if (newLevel < 0 || newLevel > 8) {
                ctx.status(400).json(Map.of("error", "level must be between 0 and 8"));
                return;
            }

            currentLevel.set(newLevel);
            ctx.json(Map.of("level", currentLevel.get()));
        });
    }
}
