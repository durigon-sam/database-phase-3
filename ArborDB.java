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

        if (connected){
                        System.out.println("Already connected to a database.");
                        try {
                            System.out.println("Press Enter to Continue");
                            System.in.read();
                        } catch (Exception e) {

                        }
        } else connect();

        System.out.println("\n**********Welcome to the ArborDB menu**********. \n\n Please choose from the provided list of methods:");
        while(true){
            printMenu();
            if (!connected) {
                System.out.println("Warning! Not connected to ArborDB, please try again by entering (1)\n");
            }
            Scanner input = new Scanner(System.in);
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
                    runAddForest(input);
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
                    runListSensors(input);
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

            try {
                System.out.println();
                System.out.println("Enter to continue...\n");
                System.in.read();
            } catch (Exception e) {

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
    static void runAddForest(Scanner scanner){
        if (!connected) {
            System.out.println("Not connected to ArborDB. Please establish a connection first.");
            return;
        }


        try {
            String sql = "INSERT INTO arbor_db.FOREST (forest_no, name, area, acid_level, MBR_XMin, MBR_XMax, MBR_YMin, MBR_YMax) " +
                         "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
            
            try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
                System.out.print("Enter forest_no (integer PRIMARY KEY): ");
                preparedStatement.setInt(1, scanner.nextInt());

                System.out.print("Enter name (varchar(30)): ");
                preparedStatement.setString(2, scanner.next());

                System.out.print("Enter area (integer): ");
                preparedStatement.setInt(3, scanner.nextInt());

                System.out.print("Enter acid_level (real): ");
                preparedStatement.setFloat(4, scanner.nextFloat());

                System.out.print("Enter MBR_XMin (real): ");
                preparedStatement.setFloat(5, scanner.nextFloat());

                System.out.print("Enter MBR_XMax (real): ");
                preparedStatement.setFloat(6, scanner.nextFloat());

                System.out.print("Enter MBR_YMin (real): ");
                preparedStatement.setFloat(7, scanner.nextFloat());

                System.out.print("Enter MBR_YMax (real): ");
                preparedStatement.setFloat(8, scanner.nextFloat());

                int rowsAffected = preparedStatement.executeUpdate();

                if (rowsAffected > 0) {
                    System.out.println("Forest added successfully!");
                } else {
                    System.out.println("Failed to add Forest. No rows affected.");
                }
            }
        } catch (SQLException e) {
            System.out.println("Error adding Forest: " + e.getMessage());
        } finally {
            // Close the scanner
        }

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
    static void runListSensors(Scanner scanner){
        if (!connected) {
            System.out.println("Not connected to ArborDB. Please establish a connection first.");
            return;
        }

        try {

            // Call the PostgreSQL function using CallableStatement
            String sql = "SELECT * FROM listSensors(?)";
            try (PreparedStatement callableStatement = conn.prepareCall(sql)) {
                System.out.print("Enter forest_no: ");
                callableStatement.setInt(1, scanner.nextInt());

                System.out.println();
                // Execute the query
                boolean hasResults = callableStatement.execute();

                // Process the results if any
                if (hasResults) {
                    try (ResultSet resultSet = callableStatement.getResultSet()) {
                        ResultSetMetaData metaData = resultSet.getMetaData();
                        int columnCount = metaData.getColumnCount(); // was used for an older version. 

                        System.out.printf("%-12s%-22s%-9s%-22s%-4s%-4s%-12s",metaData.getColumnName(1),metaData.getColumnName(2),metaData.getColumnName(3),metaData.getColumnName(4),metaData.getColumnName(5),metaData.getColumnName(6),metaData.getColumnName(7));
                        System.out.println();
                        while (resultSet.next()) {
 
                            System.out.printf("%-12s%-22s%-9s%-22s%-4s%-4s%-12s", resultSet.getString(1), resultSet.getString(2), resultSet.getString(3), resultSet.getString(4), resultSet.getString(5), resultSet.getString(6), resultSet.getString(7));
                            System.out.println();
                        }
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("Error listing sensors in the forest: " + e.getMessage());
        }

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

    public static void printMenu() {
        System.out.println("\nMenu:");
        System.out.println("+---+------------------------+------------------------------+----------------------------+");
        System.out.println("| 1 | Connect                | 8  | generateReport          | 15 | listMaintainedSensors |");
        System.out.println("| 2 | addForest              | 9  | removeSpeciesFromForest | 16 | locateTreeSpecies     |");
        System.out.println("| 3 | addTreeSpecies         | 10 | deleteWorker            | 17 | rankForestSensors     |");
        System.out.println("| 4 | addSpeciesToForest     | 11 | moveSensor              | 18 | habitableEnvironment  |");
        System.out.println("| 5 | newWorker              | 12 | removeWorkerFromState   | 19 | topSensors            |");
        System.out.println("| 6 | employWorkerToState    | 13 | removeSensor            | 20 | threeDegrees          |");
        System.out.println("| 7 | placeSensor            | 14 | listSensors             | 21 | Exit                  |");
        System.out.println("+---+------------------------+------------------------------+----------------------------+");
        System.out.println("Please enter your selection: \n");
    }

}