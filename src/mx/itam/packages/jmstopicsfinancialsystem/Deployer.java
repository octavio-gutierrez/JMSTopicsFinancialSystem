package mx.itam.packages.jmstopicsfinancialsystem;

public class Deployer {

    public static void main(String[] args) {
        int floorBrokers = 5;
        for (int i = 0; i < floorBrokers; i++) {
            new FloorBroker().start();
        }
    }

}
