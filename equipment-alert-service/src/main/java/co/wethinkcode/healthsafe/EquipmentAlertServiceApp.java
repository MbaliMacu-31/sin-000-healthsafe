package co.wethinkcode.healthsafe;

import co.wethinkcode.healthsafe.mq.MqConfig;
import io.javalin.Javalin;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

public class EquipmentAlertServiceApp {

    public static void main(String[] args) throws Exception {
        Javalin app = Javalin.create().start(7034);

        app.get("/health", ctx -> ctx.result("OK"));

        startConsumer();
    }

    private static void startConsumer() throws JMSException {
        ConnectionFactory factory = new ActiveMQConnectionFactory(MqConfig.BROKER_URL);
        Connection connection = factory.createConnection();
        connection.start();

        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Queue queue = session.createQueue(MqConfig.QUEUE);
        MessageConsumer consumer = session.createConsumer(queue);

        consumer.setMessageListener(message -> {
            try {
                if (message instanceof TextMessage textMessage) {
                    System.out.println("EQUIPMENT FAILURE ALERT: " + textMessage.getText());
                }
                message.acknowledge();
            } catch (JMSException e) {
                System.err.println("Error handling alert: " + e.getMessage());
            }
        });

        System.out.println("Consuming " + MqConfig.QUEUE + " — waiting for alerts.");
    }
}