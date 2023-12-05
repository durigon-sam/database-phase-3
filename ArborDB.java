import java.util.InputMismatchException;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.sql.*;
import java.util.Scanner;


public class ArborDB{

    public static Connection conn = null;
    public static boolean connected = false;
    
    public static void main(String args[]){

        // Ensure that the JDBC driver library is correctly loaded in
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException err) {
            System.err.println("Unable to detect the JDBC .jar dependency. Check that the library is correctly loaded in and try again.");
            System.exit(-1);
        }

        Scanner input = new Scanner(System.in);
        System.out.println("\n**********Welcome to the ArborDB menu**********. \n\n Please choose from the provided list of methods:");
        while(true){
            System.out.println("\n1. Connect - Connect to Database\n2. addForest - adds a forest to the database\n3. addTreeSpecies\n4. addSpeciesToForest\n5. newWorker\n6. employWorkerToState\n7. placeSensor\n8. generateReport\n9. removeSpeciesFromForest\n10. deleteWorker\n11. moveSensor\n12. removeWorker\n13. removeSensor\n14. listSensors\n15. listMaintainedSensors\n16. locateTreeSpecies\n17. rankForestSensors\n18. habitableEnvironment\n19. topSensors\n20. threeDegrees\n21. Exit");
            String func = input.next();
            switch (func) {
                case "1":
                    if (connected){
                        System.out.println("Already connected to a database.");
                        try {
                            System.out.println("Press Enter to Continue");
                            System.in.read();
                        } catch (Exception e) {

                        }
                    } else connect();
                    break;

                case "2":
                    runAddForest();
                    break;

                case "3":
                    runAddTreeSpecies();
                    break;

                case "4":
                    runAddSpeciesToForest();
                    break;

                case "5":
                    runNewWorker();
                    break;

                case "6":
                    runEmployWorkerToState();
                    break;

                case "7":
                    runPlaceSensor();
                    break;

                case "8":
                    runGenerateReport();
                    break;

                case "9":
                    runRemoveSpeciesFromForest();
                    break;

                case "10":
                    runDeleteWorker();
                    break;

                case "11":
                    runMoveSensor();
                    break;

                case "12":
                    runRemoveWorkerFromState();
                    break;

                case "13":
                    runRemoveSensor();
                    break;

                case "14":
                    runListSensors();
                    break;

                case "15":
                    runListMaintainedSensors();
                    break;

                case "16":
                    runLocateTreeSpecies();
                    break;

                case "17":
                    runRankForestSensors();
                    break;

                case "18":
                    runHabitableEnvironment();
                    break;
                    
                case "19":
                    runTopSensors();
                    break;

                case "20":
                    runThreeDegrees();
                    break;

                case "21": // Exit
                    Exit();
                    break;

                default:
                    System.out.println("Please input a valid function.");
                    break;
            }
        }
    }

    // TODO: check for all errors here
    static void connect(){

        Scanner input = new Scanner(System.in);
        String url = "jdbc:postgresql://localhost:5432/";
        Properties props = new Properties();

        try {
            System.out.println("Enter the username for connecting to the database: ");
            String username = input.nextLine();
            System.out.println("Enter the password for connecting to the database: ");
            String password = input.nextLine();

            props.setProperty("user", username);
            props.setProperty("password", password);
            props.setProperty("escapeSyntaxCallMode", "callIfNoReturn");

        } catch (NoSuchElementException ex) {
            System.err.println("No lines were read from user input, please try again.");
            System.exit(-1);
        } catch (IllegalArgumentException ex) {
            System.err.println("The scanner was likely closed before reading the user's input, please try again.");
            System.exit(-1);
        }

        // Connect to the ArborDB database
        try {
            conn = DriverManager.getConnection(url, props);
            System.out.println("connected to ArborDB");
            try {
                System.out.println("Press Enter to Continue");
                System.in.read();
            } catch (Exception e) {

            }
            connected = true;
        } catch (SQLException err) {
            System.out.println("SQL Error");
            while (err != null) {
                System.out.println("Message = " + err.getMessage());
                System.out.println("SQLState = " + err.getSQLState());
                System.out.println("SQL Code = " + err.getErrorCode());
                err = err.getNextException();
            }
        }
        return;
    }

    //TODO: Implement runAddForest()
    static void runAddForest(){
        return;
    }

    //TODO: Implement runAddTreeSpecies()
    static void runAddTreeSpecies(){
        return;
    }

    //TODO: Implement runAddSpeciesToForest()
    static void runAddSpeciesToForest(){
        return;
    }

    //TODO: Implement runNewWorker()
    static void runNewWorker(){
        return;
    }

    //TODO: Implement runEmployWorkerToState()
    static void runEmployWorkerToState(){
        return;
    }

    //TODO: Implement runPlaceSensor()
    static void runPlaceSensor(){
        return;
    }

    //TODO: Implement runGenerateReport()
    static void runGenerateReport(){
        return;
    }

    //TODO: Implement runRemoveSpeciesFromForest()
    static void runRemoveSpeciesFromForest(){
        return;
    }

    //TODO: Implement runDeleteWorker()
    static void runDeleteWorker(){
        return;
    }

    //TODO: Implement runMoveSensor()
    static void runMoveSensor(){
        return;
    }

    //TODO: Implement runRemoveWorkerFromState()
    static void runRemoveWorkerFromState(){
        return;
    }

    //TODO: Implement runRemoveSensor()
    static void runRemoveSensor(){
        return;
    }

    //TODO: Implement runListSensors()
    static void runListSensors(){
        return;
    }

    //TODO: Implement runListMaintainedSensors()
    static void runListMaintainedSensors(){
        return;
    }

    //TODO: Implement runLocateTreeSpecies()
    static void runLocateTreeSpecies(){
        return;
    }

    //TODO: Implement runRankForestSensors()
    static void runRankForestSensors(){
        return;
    }

    //TODO: Implement runHabitableEnvironment()
    static void runHabitableEnvironment(){
        return;
    }

    //TODO: Implement runTopSensors()
    static void runTopSensors(){
        return;
    }

    //TODO: Implement runThreeDegrees()
    static void runThreeDegrees(){
        return;
    }

    // Done
    static void Exit(){
        try{
            conn.close();
        } catch (Exception e){
            System.out.println("No database connection found");
        }
        System.out.println("Thank you for using the ArborDB database! Goodbye!");
        System.exit(0);
    }
}