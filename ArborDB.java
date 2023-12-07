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
                    runAddTreeSpecies(input);
                    break;

                case "4":
                    runAddSpeciesToForest(input);
                    break;

                case "5":
                    runNewWorker(input);
                    break;

                case "6":
                    runEmployWorkerToState(input);
                    break;

                case "7":
                    runPlaceSensor(input);
                    break;

                case "8":
                    runGenerateReport(input);
                    break;

                case "9":
                    runRemoveSpeciesFromForest(input);
                    break;

                case "10":
                    runDeleteWorker(input);
                    break;

                case "11":
                    runMoveSensor(input);
                    break;

                case "12":
                    runRemoveWorkerFromState(input);
                    break;

                case "13":
                    runRemoveSensor(input);
                    break;

                case "14":
                    runListSensors(input);
                    break;

                case "15":
                    runListMaintainedSensors(input);
                    break;

                case "16":
                    runLocateTreeSpecies(input);
                    break;

                case "17":
                    runRankForestSensors(input);
                    break;

                case "18":
                    runHabitableEnvironment(input);
                    break;
                    
                case "19":
                    runTopSensors(input);
                    break;

                case "20":
                    runThreeDegrees(input);
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

    static void runAddTreeSpecies(Scanner scanner){
        // check if connected first
        if (!connected) {
            System.out.println("Not connected to ArborDB. Please establish a connection first.");
            return;
        }
        // try catch
        try {
            // create variables for method inputs
            String genus = "";
            String epithet = "";
            Float temp = 0f;
            Float height = 0f;
            String form = "";
            
            try {
                // prompt user for the args and asign
                System.out.print("Enter genus (varchar(30)): ");
                genus = scanner.next();

                System.out.print("Enter epithet (varchar(30)): ");
                epithet = scanner.next();

                System.out.print("Enter ideal_temperature (real): ");
                temp = scanner.nextFloat();

                System.out.print("Enter largest_height (real): ");
                height = scanner.nextFloat();

                System.out.print("Enter raunkiaer_life_form (varchar(16)): ");
                form = scanner.next();
            } catch (InputMismatchException e) {
                System.out.println("Mismatched input type.");
                return;
            } catch (NoSuchElementException e1){
                System.err.println("No lines were read from user input, please try again.");
                return;
            }

            // configure the procedure call
            try (CallableStatement callableStatement = conn.prepareCall("{ call addTreeSpecies( ?,?,?,?,? ) }")) {

                callableStatement.setString(1, genus);
                callableStatement.setString(2, epithet);
                callableStatement.setFloat(3, temp);
                callableStatement.setFloat(4, height);
                callableStatement.setString(5, form);

                // call it
                callableStatement.execute();
            }
            
        } catch (SQLException e) {
            System.out.println("Error: " + e.getMessage());
        }
        // return after adding
        return;
    }

    static void runAddSpeciesToForest(Scanner scanner){
        // check if connected first
        if (!connected) {
            System.out.println("Not connected to ArborDB. Please establish a connection first.");
            return;
        }
        // try catch
        try {
            // create variables for method inputs
            int forestNo = 0;
            String genus = "";
            String epithet = "";

            try {
                // prompt user for the args and asign
                System.out.print("Enter forest_no (integer): ");
                forestNo = scanner.nextInt();

                System.out.print("Enter genus (varchar(30)): ");
                genus = scanner.next();

                System.out.print("Enter epithet (varchar(30)): ");
                epithet = scanner.next();
            } catch (InputMismatchException e) {
                System.out.println("Mismatched input type.");
                return;
            } catch (NoSuchElementException e1){
                System.err.println("No lines were read from user input, please try again.");
                return;
            }

            // configure the procedure call
            try (CallableStatement callableStatement = conn.prepareCall(" { call addSpeciesToForest( ?,?,? ) }")) {
                
                callableStatement.setInt(1, forestNo);
                callableStatement.setString(2, genus);
                callableStatement.setString(3, epithet);

                // call it
                callableStatement.execute();
            }
        } catch (SQLException e) {
            System.out.println("Error: " + e.getMessage());
        }
        // return after adding
        return;
    }

    static void runNewWorker(Scanner scanner){
        // check if connected first
        if (!connected) {
            System.out.println("Not connected to ArborDB. Please establish a connection first.");
            return;
        }
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
        }
        // return after adding
        return;
    }

    static void runEmployWorkerToState(Scanner scanner){
        // check if connected first
        if (!connected) {
            System.out.println("Not connected to ArborDB. Please establish a connection first.");
            return;
        }
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
        }
        // return after adding
        return;
    }

    //TODO: Implement runPlaceSensor()
    static void runPlaceSensor(Scanner scanner){
        // check if connected first
        if (!connected) {
            System.out.println("Not connected to ArborDB. Please establish a connection first.");
            return;
        }
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
        }
        // return after adding
        return;
    }

    //TODO: Implement runGenerateReport()
    static void runGenerateReport(Scanner scanner){
        // check if connected first
        if (!connected) {
            System.out.println("Not connected to ArborDB. Please establish a connection first.");
            return;
        }
        // try catch
        try {
            // first display list of all sensors
            String sql = "SELECT * FROM arbor_db.SENSOR";

            try (PreparedStatement preparedStatement = conn.prepareCall(sql)) {

            }

        } catch (SQLException e) {
            System.out.println("Error: " + e.getMessage());
        }
        // return after adding
        return;
    }

    static void runRemoveSpeciesFromForest(Scanner scanner){
        // check if connected first
        if (!connected) {
            System.out.println("Not connected to ArborDB. Please establish a connection first.");
            return;
        }
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
        }
        // return after removing
        return;
    }

    static void runDeleteWorker(Scanner scanner){
        // check if connected first
        if (!connected) {
            System.out.println("Not connected to ArborDB. Please establish a connection first.");
            return;
        }
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
        }
        // return after removing
        return;
    }

    static void runMoveSensor(Scanner scanner){
        // check if connected first
        if (!connected) {
            System.out.println("Not connected to ArborDB. Please establish a connection first.");
            return;
        }
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
        }
        // return after updating
        return;
    }

    //TODO: Implement runRemoveWorkerFromState()
    static void runRemoveWorkerFromState(Scanner scanner){
        // check if connected first
        if (!connected) {
            System.out.println("Not connected to ArborDB. Please establish a connection first.");
            return;
        }
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
        }
        // return after removing
        return;
    }

    //TODO: Implement runRemoveSensor()
    static void runRemoveSensor(Scanner scanner){
        // check if connected first
        if (!connected) {
            System.out.println("Not connected to ArborDB. Please establish a connection first.");
            return;
        }
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
        }
        // return after adding
        return;
    }

    //TODO: Implement runListSensors()
    static void runListSensors(Scanner scanner){
        if (!connected) {
            System.out.println("Not connected to ArborDB. Please establish a connection first.");
            return;
        }

        try {
            
            String sql = "SELECT * FROM listSensors(?)";
            try (PreparedStatement preparedStatement = conn.prepareCall(sql)) {
                clearScreen(20);
                System.out.println("Retrieve list of sensors");
                System.out.print("Enter forest_no: ");
                preparedStatement.setInt(1, scanner.nextInt());

                System.out.println();
                // Execute the query
                boolean hasResults = preparedStatement.execute();

                // Process the results if any
                if (hasResults) {
                    try (ResultSet resultSet = preparedStatement.getResultSet()) {
                        if (resultSet.next()) {

                            ResultSetMetaData metaData = resultSet.getMetaData();
                            int columnCount = metaData.getColumnCount(); // was used for an older version. 
    
                            System.out.printf("%-12s%-22s%-9s%-22s%-4s%-4s%-12s",metaData.getColumnName(1),metaData.getColumnName(2),metaData.getColumnName(3),metaData.getColumnName(4),metaData.getColumnName(5),metaData.getColumnName(6),metaData.getColumnName(7));
                            System.out.println();
                            while (resultSet.next()) {
     
                                System.out.printf("%-12s%-22s%-9s%-22s%-4s%-4s%-12s", resultSet.getString(1), resultSet.getString(2), resultSet.getString(3), resultSet.getString(4), resultSet.getString(5), resultSet.getString(6), resultSet.getString(7));
                                System.out.println();
                            }
                        }
                        else {
                            System.out.println("[ERROR] Empty or invalid forest_no.");
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
    static void runListMaintainedSensors(Scanner scanner){
        return;
    }

    //TODO: Implement runLocateTreeSpecies()
    static void runLocateTreeSpecies(Scanner scanner){
        return;
    }

    //TODO: Implement runRankForestSensors()
    static void runRankForestSensors(Scanner scanner){
        return;
    }

    //TODO: Implement runHabitableEnvironment()
    static void runHabitableEnvironment(Scanner scanner){
        return;
    }

    //TODO: Implement runTopSensors()
    static void runTopSensors(Scanner scanner){
        return;
    }

    //TODO: Implement runThreeDegrees()
    static void runThreeDegrees(Scanner scanner){
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

    public static void clearScreen(int n) {
        for (int i = 0; i < n; i++){
            System.out.println();
        }
    }
}