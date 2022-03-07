package mx.itam.packages.jmstopicsfinancialsystem;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Topic;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.Random;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

public class FloorBroker extends Thread {

    // URL of the JMS server
    private static String url = ActiveMQConnection.DEFAULT_BROKER_URL;
    // default broker URL is : tcp://localhost:61616"

    @Override
    public void run() {
        try {
            ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(url);
            Connection connection = connectionFactory.createConnection();
            connection.start();

            MessageConsumer messageConsumer;
            TextMessage textMessage;
            boolean endFinancialSession = false;

            Session session = connection.createSession(false /*Transacter*/, Session.AUTO_ACKNOWLEDGE);

            String newsTypes[] = new String[5];
            newsTypes[0] = "Telecommunications";
            newsTypes[1] = "Banks";
            newsTypes[2] = "Transportation";
            newsTypes[3] = "FoodSupply";
            newsTypes[4] = "Education";
            Random randomGenerator = new Random();
            int myTopic = randomGenerator.nextInt(5);
            Topic topic = session.createTopic(newsTypes[myTopic]);
            System.out.println("I'm a floor broker handling " + newsTypes[myTopic] + " accounts");
            messageConsumer = session.createConsumer(topic);
            connection.start();
            while (!endFinancialSession) {
                textMessage = (TextMessage) messageConsumer.receive();
                if (textMessage != null) {
                    if (!textMessage.getText().trim().equals("The End")) {
                        System.out.println("I received bad news of level: " + textMessage.getText());
                        if (Integer.parseInt(textMessage.getText()) > 5) {
                            System.out.println("Selling! Selling! Selling!");
                        } else {
                            System.out.println("I have to be patient. There is no such thing as a 'global economic crisis'");
                        }
                    }
                    if (textMessage.getText().trim().equals("The End")) {
                        endFinancialSession = true;
                    }
                }

            }
            messageConsumer.close();
            session.close();
            connection.close();
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

}