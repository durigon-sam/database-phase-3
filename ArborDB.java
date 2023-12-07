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

    static void runAddTreeSpecies(){
        // check if connected first
        if (!connected) {
            System.out.println("Not connected to ArborDB. Please establish a connection first.");
            return;
        }
        // create a new scanner for inputs
        Scanner scanner = new Scanner(System.in);
        // try catch
        try {
            String sql = "INSERT INTO arbor_db.TREE_SPECIES (genus, epithet, ideal_temperature, largest_height, raunkiaer_life_form) " +
            "VALUES (?, ?, ?, ?, ?)";

            try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
                // prompt user for the args
                System.out.print("Enter genus (varchar(30)): ");
                preparedStatement.setString(1, scanner.next());

                System.out.print("Enter epithet (varchar(30)): ");
                preparedStatement.setString(2, scanner.next());

                System.out.print("Enter ideal_temperature (real): ");
                preparedStatement.setFloat(3, scanner.nextFloat());

                System.out.print("Enter largest_height (real): ");
                preparedStatement.setFloat(4, scanner.nextFloat());

                System.out.print("Enter raunkiaer_life_form (varchar(16)): ");
                preparedStatement.setString(5, scanner.next());

                // see if it worked
                int rowsAffected = preparedStatement.executeUpdate();

                if (rowsAffected > 0) {
                    System.out.println("Success!");
                } else {
                    System.out.println("Failure.");
                }
            }
        } catch (SQLException e) {
            System.out.println("Error: " + e.getMessage());
        } finally {
            // close scanner
            scanner.close();
        }
        // return after adding
        return;
    }

    static void runAddSpeciesToForest(){
        // check if connected first
        if (!connected) {
            System.out.println("Not connected to ArborDB. Please establish a connection first.");
            return;
        }
        // create a new scanner for inputs
        Scanner scanner = new Scanner(System.in);
        // try catch
        try {
            String sql = "INSERT INTO arbor_db.FOUND_IN (forest_no, genus, epithet) " +
            "VALUES (?, ?, ?)";

            try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
                // prompt user for the args
                System.out.print("Enter forest_no (integer): ");
                preparedStatement.setInt(1, scanner.nextInt());

                System.out.print("Enter genus (varchar(30)): ");
                preparedStatement.setString(2, scanner.next());

                System.out.print("Enter epithet (varchar(30)): ");
                preparedStatement.setString(3, scanner.next());

                // see if it worked
                int rowsAffected = preparedStatement.executeUpdate();

                if (rowsAffected > 0) {
                    System.out.println("Success!");
                } else {
                    System.out.println("Failure.");
                }
            }
        } catch (SQLException e) {
            System.out.println("Error: " + e.getMessage());
        } finally {
            // close scanner
            scanner.close();
        }
        // return after adding
        return;
    }

    static void runNewWorker(){
        // check if connected first
        if (!connected) {
            System.out.println("Not connected to ArborDB. Please establish a connection first.");
            return;
        }
        // create a new scanner for inputs
        Scanner scanner = new Scanner(System.in);
        // try catch
        try {
            String sql1 = "INSERT INTO arbor_db.WORKER (ssn, first, last, middle, rank) " +
            "VALUES (?, ?, ?, ?, ?)";

            String sql2 = "INSERT INTO arbor_db.EMPLOYED (state, worker) " +
            "VALUES (?, ?)";

            try (PreparedStatement preparedStatement = conn.prepareStatement(sql1)) {
                // prompt user for the args
                System.out.print("Enter ssn (char(9)): ");
                // store ssn to be used later as well
                String ssn = scanner.next();
                preparedStatement.setString(1, ssn);

                System.out.print("Enter first name (varchar(30)): ");
                preparedStatement.setString(2, scanner.next());

                System.out.print("Enter last name (varchar(30)): ");
                preparedStatement.setString(3, scanner.next());

                System.out.print("Enter middle initial (char(1)): ");
                preparedStatement.setString(4, scanner.next());

                System.out.print("Enter rank (varchar(10)): ");
                preparedStatement.setString(5, scanner.next());

                // run first sql statement
                int rowsAffected = preparedStatement.executeUpdate();
                int rowsAffected2 = 0;

                try (PreparedStatement preparedStatement2 = conn.prepareStatement(sql2)) {

                    // get the state abbr from user
                    System.out.print("Enter state abbreviation (char(2)): ");
                    preparedStatement2.setString(1, scanner.next());

                    // set ssn as second arg in statement
                    preparedStatement2.setString(2, ssn);

                    // run second sql statement
                    rowsAffected2 = preparedStatement2.executeUpdate();
                }

                if (rowsAffected > 0 && rowsAffected2 > 0) {
                    System.out.println("Success!");
                } else {
                    System.out.println("Failure.");
                }
            }
        } catch (SQLException e) {
            System.out.println("Error: " + e.getMessage());
        } finally {
            // close scanner
            scanner.close();
        }
        // return after adding
        return;
    }

    static void runEmployWorkerToState(){
        // check if connected first
        if (!connected) {
            System.out.println("Not connected to ArborDB. Please establish a connection first.");
            return;
        }
        // create a new scanner for inputs
        Scanner scanner = new Scanner(System.in);
        // try catch
        try {
            String sql = "INSERT INTO arbor_db.EMPLOYED (state, worker) " +
            "VALUES (?, ?)";

            try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
                // prompt user for the args
                System.out.print("Enter state abbreviation (char(2)): ");
                preparedStatement.setString(1, scanner.next());

                System.out.print("Enter ssn (char(9)): ");
                preparedStatement.setString(2, scanner.next());

                // see if it worked
                int rowsAffected = preparedStatement.executeUpdate();

                if (rowsAffected > 0) {
                    System.out.println("Success!");
                } else {
                    System.out.println("Failure.");
                }
            }
        } catch (SQLException e) {
            System.out.println("Error: " + e.getMessage());
        } finally {
            // close scanner
            scanner.close();
        }
        // return after adding
        return;
    }

    //TODO: Implement runPlaceSensor()
    static void runPlaceSensor(){
        // check if connected first
        if (!connected) {
            System.out.println("Not connected to ArborDB. Please establish a connection first.");
            return;
        }
        // create a new scanner for inputs
        Scanner scanner = new Scanner(System.in);
        // try catch
        try {
            String sql = "INSERT INTO arbor_db.SENSOR (sensor_id, last_charged, energy, last_read, x, y, maintainer_id) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?)";

            try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
                // prompt user for the args
                System.out.print("Enter energy (integer): ");
                preparedStatement.setInt(3, scanner.nextInt());

                System.out.print("Enter x of location of deployment (real): ");
                preparedStatement.setFloat(5, scanner.nextFloat());

                System.out.print("Enter y of location of deployment (real): ");
                preparedStatement.setFloat(6, scanner.nextFloat());

                System.out.print("Enter maintainer id (char(9)): ");
                preparedStatement.setString(7, scanner.next());

                // TODO: calculate last_read and last_charge. Use current time from clock relation
                String lastRead = "";
                String lastCharged = "";

                String sql2 = "SELECT synthetic_time INTO time FROM arbor_db.CLOCK";

                // run the above line to get current time

                // run the entire insert statement
                int rowsAffected = preparedStatement.executeUpdate();

                if (rowsAffected > 0) {
                    System.out.println("Success!");
                } else {
                    System.out.println("Failure.");
                }
            }
        } catch (SQLException e) {
            System.out.println("Error: " + e.getMessage());
        } finally {
            // close scanner
            scanner.close();
        }
        // return after adding
        return;
    }

    //TODO: Implement runGenerateReport()
    static void runGenerateReport(){
        // check if connected first
        if (!connected) {
            System.out.println("Not connected to ArborDB. Please establish a connection first.");
            return;
        }
        // create a new scanner for inputs
        Scanner scanner = new Scanner(System.in);
        // try catch
        try {
            // first display list of all sensors
            String sql = "SELECT * FROM arbor_db.SENSOR";

            try (PreparedStatement preparedStatement = conn.prepareCall(sql)) {

            }

        } catch (SQLException e) {
            System.out.println("Error: " + e.getMessage());
        } finally {
            // close scanner
            scanner.close();
        }
        // return after adding
        return;
    }

    static void runRemoveSpeciesFromForest(){
        // check if connected first
        if (!connected) {
            System.out.println("Not connected to ArborDB. Please establish a connection first.");
            return;
        }
        // create a new scanner for inputs
        Scanner scanner = new Scanner(System.in);
        // try catch
        try {
            String sql = "DELETE FROM arbor_db.FOUND_IN WHERE forest_no = ? AND genus = ? AND epithet = ?";

            try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
                // prompt user for the args
                System.out.print("Enter forest_no (integer): ");
                preparedStatement.setInt(1, scanner.nextInt());

                System.out.print("Enter genus (varchar(30)): ");
                preparedStatement.setString(2, scanner.next());

                System.out.print("Enter epithet (varchar(30)): ");
                preparedStatement.setString(3, scanner.next());

                // see if it worked
                int rowsAffected = preparedStatement.executeUpdate();

                if (rowsAffected > 0) {
                    System.out.println("Success!");
                } else {
                    System.out.println("Failure.");
                }
            }
        } catch (SQLException e) {
            System.out.println("Error: " + e.getMessage());
        } finally {
            // close scanner
            scanner.close();
        }
        // return after removing
        return;
    }

    static void runDeleteWorker(){
        // check if connected first
        if (!connected) {
            System.out.println("Not connected to ArborDB. Please establish a connection first.");
            return;
        }
        // create a new scanner for inputs
        Scanner scanner = new Scanner(System.in);
        // try catch
        try {
            String sql3 = "DELETE FROM arbor_db.WORKER WHERE ssn = ?";
            String sql2 = "DELETE FROM arbor_db.EMPLOYED WHERE worker = ?";
            String sql1 = "DELETE FROM arbor_db.SENSOR WHERE maintainer_id = ?";

            try (PreparedStatement preparedStatement1 = conn.prepareStatement(sql1)) {
                // prompt user for the arg
                System.out.print("Enter ssn (char(9)): ");
                // store the ssn 
                String ssn = scanner.next();

                preparedStatement1.setString(1, ssn);

                // run first sql statement
                int rowsAffected1 = preparedStatement1.executeUpdate();
                int rowsAffected2 = 0;
                int rowsAffected3 = 0;

                try (PreparedStatement preparedStatement2 = conn.prepareStatement(sql2)) {
                    // set ssn as arg
                    preparedStatement2.setString(1, ssn);

                    // run second sql statement
                    rowsAffected2 = preparedStatement2.executeUpdate();

                    try (PreparedStatement preparedStatement3 = conn.prepareStatement(sql3)) {
                        // set ssn as arg
                        preparedStatement3.setString(1, ssn);

                        // run third sql statement
                        rowsAffected3 = preparedStatement3.executeUpdate();
                    }
                }

                // see if worked for all 3
                if (rowsAffected1 > 0 && rowsAffected2 > 0 && rowsAffected3 > 0) {
                    System.out.println("Success!");
                } else {
                    System.out.println("Failure.");
                }
            }
        } catch (SQLException e) {
            System.out.println("Error: " + e.getMessage());
        } finally {
            // close scanner
            scanner.close();
        }
        // return after removing
        return;
    }

    static void runMoveSensor(){
        // check if connected first
        if (!connected) {
            System.out.println("Not connected to ArborDB. Please establish a connection first.");
            return;
        }
        // create a new scanner for inputs
        Scanner scanner = new Scanner(System.in);
        // try catch
        try {
            String sql = "UPDATE arbor_db.SENSOR " +
            "SET X = ?, Y = ? " +
            "WHERE sensor_id = ?";

            try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
                // first check if there are any sensors to change
                String sql2 = "SELECT * FROM arbor_db.SENSOR";
                try (PreparedStatement preparedStatement2 = conn.prepareStatement(sql2)) {
                    // Execute the query
                    boolean hasResults = preparedStatement2.execute();
                    // check result
                    if (hasResults) {
                        try (ResultSet resultSet = preparedStatement2.getResultSet()) {
                            if (resultSet.next()) {
                                // there are sensors, do all the work
                                // prompt user for the args
                                System.out.print("Enter sensor_id (integer): ");
                                int id = scanner.nextInt();

                                // if user enters -1 as sensor_id, return to menu
                                if (id == -1) {
                                    return;
                                }

                                // if not -1, continue
                                preparedStatement.setInt(3, id);

                                System.out.print("Enter new X location (real): ");
                                preparedStatement.setFloat(1, scanner.nextFloat());

                                System.out.print("Enter new Y location (real): ");
                                preparedStatement.setFloat(2, scanner.nextFloat());

                                // see if it worked
                                int rowsAffected = preparedStatement.executeUpdate();

                                if (rowsAffected > 0) {
                                    System.out.println("Success!");
                                } else {
                                    System.out.println("Failure.");
                                }
                                return;
                            }
                            else {
                                // no sensors no print and return
                                System.out.print("No Sensors to Redeploy");
                                return;
                            }
                        }
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("Error: " + e.getMessage());
        } finally {
            // close scanner
            scanner.close();
        }
        // return after updating
        return;
    }

    //TODO: Implement runRemoveWorkerFromState()
    static void runRemoveWorkerFromState(){
        // check if connected first
        if (!connected) {
            System.out.println("Not connected to ArborDB. Please establish a connection first.");
            return;
        }
        // create a new scanner for inputs
        Scanner scanner = new Scanner(System.in);
        // try catch
        try {
            String sql = "DELETE FROM arbor_db.EMPLOYED " +
            "WHERE worker = ? " +
            "AND state = ?";

            try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
                // prompt user for the args
                System.out.print("Enter ssn (char(9)): ");
                preparedStatement.setString(1, scanner.next());

                System.out.print("Enter state abbreviation (char(2)): ");
                preparedStatement.setString(2, scanner.next());

                // run sql and delete from the table
                int rowsAffected = preparedStatement.executeUpdate();

                // TODO: REPLACEMENT WORKER

                if (rowsAffected > 0) {
                    System.out.println("Success!");
                } else {
                    System.out.println("Failure.");
                }
            }
        } catch (SQLException e) {
            System.out.println("Error: " + e.getMessage());
        } finally {
            // close scanner
            scanner.close();
        }
        // return after removing
        return;
    }

    //TODO: Implement runRemoveSensor()
    static void runRemoveSensor(){
        // check if connected first
        if (!connected) {
            System.out.println("Not connected to ArborDB. Please establish a connection first.");
            return;
        }
        // create a new scanner for inputs
        Scanner scanner = new Scanner(System.in);
        // try catch
        try {
            String sql = "";

            try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
                // prompt user for the args
                System.out.print("Would you like to remove ALL or SELECTED sensors from ArborDB?");

                // see if it worked
                int rowsAffected = preparedStatement.executeUpdate();

                // TODO: rest

                if (rowsAffected > 0) {
                    System.out.println("Success!");
                } else {
                    System.out.println("Failure.");
                }
            }
        } catch (SQLException e) {
            System.out.println("Error: " + e.getMessage());
        } finally {
            // close scanner
            scanner.close();
        }
        // return after adding
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