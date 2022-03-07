package mx.itam.packages.jmstopicsfinancialsystem;

import java.util.Random;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Topic;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

public class InformationProvider {

    // URL of the JMS server
    private static String url = ActiveMQConnection.DEFAULT_BROKER_URL;
    // default broker URL is : tcp://localhost:61616"

    private int maxNumberOfNews = 5;
    private int avgInterarrivalTime = 1000; // in ms

    public void produceNews() {
        MessageProducer messageProducer;
        TextMessage textMessage;
        try {
            ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(url);
            Connection connection = connectionFactory.createConnection();
            connection.start();
            Session session = connection.createSession(false /*Transacter*/, Session.AUTO_ACKNOWLEDGE);

            String newsTypes[] = new String[5];
            newsTypes[0] = "Telecommunications";
            newsTypes[1] = "Banks";
            newsTypes[2] = "Transportation";
            newsTypes[3] = "FoodSupply";
            newsTypes[4] = "Education";

            for (int i = 0; i < maxNumberOfNews; i++) {
                Random randomGenerator = new Random();
                int myTopic = randomGenerator.nextInt(5);
                Topic topic = session.createTopic(newsTypes[myTopic]);
                messageProducer = session.createProducer(topic);
                textMessage = session.createTextMessage();
                textMessage.setText(String.valueOf(randomGenerator.nextInt(10) + 1)); // random number from 1 to 10 where 10 represents the worst news

                long delay = (long) (avgInterarrivalTime * (-Math.log(Math.random()))); //  Arrival process is Poisson Distributed
                try {
                    System.out.println("Sending market news. Level: " + textMessage.getText() + " Category: " + newsTypes[myTopic]);
                    messageProducer.send(textMessage);
                    Thread.sleep((long) delay);

                } catch (Exception e) {
                    e.printStackTrace();
                }

                if ((i + 1) == maxNumberOfNews) {
                    for (int j = 0; j < newsTypes.length; j++) {
                        Topic innerTopic = session.createTopic(newsTypes[j]);
                        messageProducer = session.createProducer(innerTopic);
                        textMessage = session.createTextMessage();
                        textMessage.setText("The End");
                        System.out.println("Notifying the end of the financial session to " + newsTypes[j] + " Floor Brokers");
                        messageProducer.send(textMessage);
                    }
                }
                messageProducer.close();
            }
            session.close();
            connection.close();
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        new InformationProvider().produceNews();
    }
}