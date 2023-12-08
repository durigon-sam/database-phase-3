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
            // create variables for method inputs
            String name = "";
            int area = 0;
            Float acidLevel = 0f;
            Float xmin = 0f;
            Float xmax = 0f;
            Float ymin = 0f;
            Float ymax = 0f;

            try {
                // prompt user for the args and assign                
                System.out.print("Enter name (varchar(30)): ");
                name = scanner.next();

                System.out.print("Enter area (integer): ");
                area = scanner.nextInt();

                System.out.print("Enter acid_level (real): ");
                acidLevel = scanner.nextFloat();

                System.out.print("Enter MBR_XMin (real): ");
                xmin = scanner.nextFloat();

                System.out.print("Enter MBR_XMax (real): ");
                xmax = scanner.nextFloat();

                System.out.print("Enter MBR_YMin (real): ");
                ymin = scanner.nextFloat();

                System.out.print("Enter MBR_YMax (real): ");
                ymax = scanner.nextFloat();
            } catch (InputMismatchException e) {
                System.out.println("Mismatched input type.");
                return;
            } catch (NoSuchElementException e1){
                System.err.println("No lines were read from user input, please try again.");
                return;
            }

            // configure the procedure call
            try (CallableStatement callableStatement = conn.prepareCall("{ call addForest( ?,?,?,?,?,?,? ) }")) {

                callableStatement.setString(1, name);
                callableStatement.setInt(2, area);
                callableStatement.setFloat(3, acidLevel);
                callableStatement.setFloat(4, xmin);
                callableStatement.setFloat(5, xmax);
                callableStatement.setFloat(6, ymin);
                callableStatement.setFloat(7, ymax);

                // call it
                callableStatement.execute();
            }
        } catch (SQLException e) {
            System.out.println("Error adding Forest: " + e.getMessage());
        }
        // return after adding
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
            // create variables for method inputs
            String ssn = "";
            String fName = "";
            String lName = "";
            String middle = "";
            String rank = "";
            String state = "";

            try {
                // prompt user for the args and asign
                System.out.print("Enter ssn (char(9)): ");
                ssn = scanner.next();

                System.out.print("Enter first name (varchar(30)): ");
                fName = scanner.next();

                System.out.print("Enter last name (varchar(30)): ");
                lName = scanner.next();

                System.out.print("Enter middle initial (char(1)): ");
                middle = scanner.next();

                System.out.print("Enter rank (varchar(10)): ");
                rank = scanner.next();
                
                System.out.print("Enter state abbreviation (char(2)): ");
                state = scanner.next();

            } catch (InputMismatchException e) {
                System.out.println("Mismatched input type.");
                return;
            } catch (NoSuchElementException e1){
                System.err.println("No lines were read from user input, please try again.");
                return;
            }

            // configure the procedure call
            try (CallableStatement callableStatement = conn.prepareCall(" { call newWorker( ?,?,?,?,?,? ) }")) {
                
                callableStatement.setString(1, ssn);
                callableStatement.setString(2, fName);
                callableStatement.setString(3, lName);
                callableStatement.setString(4, middle);
                callableStatement.setString(5, rank);
                callableStatement.setString(6, state);

                // call it
                callableStatement.execute();
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
            // create variables for method inputs
            String state = "";
            String ssn = "";

            try {
                // prompt user for the args and asign
                System.out.print("Enter state abbreviation (char(2)): ");
                state = scanner.next();

                System.out.print("Enter ssn (char(9)): ");
                ssn = scanner.next();

            } catch (InputMismatchException e) {
                System.out.println("Mismatched input type.");
                return;
            } catch (NoSuchElementException e1){
                System.err.println("No lines were read from user input, please try again.");
                return;
            }          
            // configure the procedure call
            try (CallableStatement callableStatement = conn.prepareCall("{ call employWorkerToState( ?,? ) }")) {

                callableStatement.setString(1, state);
                callableStatement.setString(2, ssn);

                // call it
                callableStatement.execute();
            } 
        } catch (SQLException e) {
            System.out.println("Error: " + e.getMessage());
        }
        // return after adding
        return;
    }

    //TODO: works but a sensor needs to already exist in table in order for new sensorId to be created
    static void runPlaceSensor(Scanner scanner){
        // check if connected first
        if (!connected) {
            System.out.println("Not connected to ArborDB. Please establish a connection first.");
            return;
        }
        // try catch
        try {
            // create variables for method inputs
            int energy = 0;
            Float x = 0f;
            Float y = 0f;
            String id = "";

            try {
                // prompt user for the args and assign                
                System.out.print("Enter energy (integer): ");
                energy = scanner.nextInt();

                System.out.print("Enter x of location of deployment (real): ");
                x = scanner.nextFloat();

                System.out.print("Enter y of location of deployment (real): ");
                y = scanner.nextFloat();

                System.out.print("Enter maintainer id (char(9)): ");
                id = scanner.next();

            } catch (InputMismatchException e) {
                System.out.println("Mismatched input type.");
                return;
            } catch (NoSuchElementException e1){
                System.err.println("No lines were read from user input, please try again.");
                return;
            }

            // configure the procedure call
            try (CallableStatement callableStatement = conn.prepareCall("{ call placeSensor( ?,?,?,? ) }")) {

                callableStatement.setInt(1, energy);
                callableStatement.setFloat(2, x);
                callableStatement.setFloat(3, y);
                callableStatement.setString(4, id);

                // call it
                callableStatement.execute();
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

            try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
                // execute it
                boolean hasResults = preparedStatement.execute();
                // process results, if any
                if (hasResults) {
                    try (ResultSet resultSet = preparedStatement.getResultSet()) {
                        if (resultSet.next()) {

                            ResultSetMetaData metaData = resultSet.getMetaData();
    
                            System.out.printf("%-12s%-22s%-9s%-22s%-4s%-4s%-12s",metaData.getColumnName(1),metaData.getColumnName(2),metaData.getColumnName(3),metaData.getColumnName(4),metaData.getColumnName(5),metaData.getColumnName(6),metaData.getColumnName(7));
                            System.out.println();

                            while (resultSet.next()) {
                                System.out.printf("%-12s%-22s%-9s%-22s%-4s%-4s%-12s", resultSet.getString(1), resultSet.getString(2), resultSet.getString(3), resultSet.getString(4), resultSet.getString(5), resultSet.getString(6), resultSet.getString(7));
                                System.out.println();
                            }
                        }
                    }
                    // continue after listing all sensors
                    // create variables to make report for
                    int sensorId = 0;
                    java.sql.Timestamp reportTime;
                    String date;
                    Float temp = 0f;

                    try {
                        System.out.println("Enter an exisiting sensor_id, or -1 to exit: ");
                        sensorId = scanner.nextInt();

                        // check sensorId result
                        if (sensorId == -1) {
                            return;
                        }

                        // ask for other inputs
                        System.out.print("Enter report time (yyyy-mm-dd hh:mm:ss): ");
                        date = scanner.next();

                        System.out.println("Date is: " + date);

                        System.out.print("Enter temperature (real): ");
                        temp = scanner.nextFloat();

                    } catch (InputMismatchException e) {
                        System.out.println("Mismatched input type.");
                        return;
                    } catch (NoSuchElementException e1){
                        System.err.println("No lines were read from user input, please try again.");
                        return;
                    }

                    // // configure the procedure call
                    // try (CallableStatement callableStatement = conn.prepareCall("{ call generateReport( ?,?,? ) }")) {

                    //     callableStatement.setInt(1, sensorId);
                    //     callableStatement.setTimestamp(2, reportTime);
                    //     callableStatement.setFloat(3, temp);

                    //     // call it
                    //     callableStatement.execute();
                    // }
                } else {
                    // TODO: no results to display, print and return
                    System.out.println("No Sensors are currently deployed.");
                    return;
                }
            }

        } catch (SQLException e) {
            System.out.println("Error: " + e.getMessage());
        }
        // return after displaying
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
            // create variables for method inputs
            int forest_no = 0;
            String genus = "";
            String epithet = "";
            
            try {
                // prompt user for the args and asign
                System.out.print("Enter forest_no (integer): ");
                forest_no = scanner.nextInt();

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
            try (CallableStatement callableStatement = conn.prepareCall("{ call removeSpeciesFromForest( ?,?,? ) }")) {

                callableStatement.setInt(1, forest_no);
                callableStatement.setString(2, genus);
                callableStatement.setString(3, epithet);
                
                // call it
                callableStatement.execute();
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
            // create variables for method inputs
            String ssn = "";
            
            try {
                // prompt user for the args and asign
                System.out.print("Enter ssn (char(9)): ");
                ssn = scanner.next();

            } catch (InputMismatchException e) {
                System.out.println("Mismatched input type.");
                return;
            } catch (NoSuchElementException e1){
                System.err.println("No lines were read from user input, please try again.");
                return;
            }

            // configure the procedure call
            try (CallableStatement callableStatement = conn.prepareCall("{ call deleteWorker( ? ) }")) {

                callableStatement.setString(1, ssn);
                
                // call it
                callableStatement.execute();
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
            int sensorId = 0;
            Float x = 0f;
            Float y = 0f;

            // first check if there are any sensors to change
            try (PreparedStatement preparedStatement = conn.prepareStatement("SELECT * FROM arbor_db.SENSOR")) {
                    // execute the query
                    boolean hasResults = preparedStatement.execute();
                    // check result
                    if (hasResults) {
                        try (ResultSet resultSet = preparedStatement.getResultSet()) {
                            if (resultSet.next()) {
                                // there are sensors, so do all the work
                                // prompt user for the args
                                System.out.print("Enter sensor_id (integer): ");
                                sensorId = scanner.nextInt();

                                // if user enters -1 as sensor_id, return to menu
                                if (sensorId == -1) {
                                    System.out.println("Exiting!");
                                    return;
                                }

                                // if not -1, continue
                                try {
                                    // prompt user for the args and assign                    
                                    System.out.print("Enter new x coord (real): ");
                                    x = scanner.nextFloat();
                    
                                    System.out.print("Enter new y coord (real): ");
                                    y = scanner.nextFloat();
                    
                                } catch (InputMismatchException e) {
                                    System.out.println("Mismatched input type.");
                                    return;
                                } catch (NoSuchElementException e1){
                                    System.err.println("No lines were read from user input, please try again.");
                                    return;
                                }
                                // configure the procedure call
                                try (CallableStatement callableStatement = conn.prepareCall("{ call moveSensor( ?,?,? ) }")) {

                                    callableStatement.setInt(1, sensorId);
                                    callableStatement.setFloat(2, x);
                                    callableStatement.setFloat(3, y);

                                    // call it
                                    callableStatement.execute();
                                }
                            }
                            else {
                                // no sensors no print and return
                                System.out.print("No Sensors to Redeploy");
                                return;
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

    static void runRemoveWorkerFromState(Scanner scanner){
        // check if connected first
        if (!connected) {
            System.out.println("Not connected to ArborDB. Please establish a connection first.");
            return;
        }
        
        // try catch
        try {
            // create variables for method inputs
            String ssn = "";
            String state = "";

            try {
                // prompt user for the args and asign
                System.out.print("Enter ssn (char(9)): ");
                ssn = scanner.next();

                System.out.print("Enter state abbreviation (char(2)): ");
                state = scanner.next();

            } catch (InputMismatchException e) {
                System.out.println("Mismatched input type.");
                return;
            } catch (NoSuchElementException e1){
                System.err.println("No lines were read from user input, please try again.");
                return;
            }

            // configure the procedure call
            try (CallableStatement callableStatement = conn.prepareCall(" { call removeWorkerFromState( ?,? ) }")) {
                
                callableStatement.setString(1, ssn);
                callableStatement.setString(2, state);

                // call it
                callableStatement.execute();
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

    //DONE: Implement runThreeDegrees()
    static void runThreeDegrees(Scanner scanner){
        // check if connected first
        if (!connected) {
            System.out.println("Not connected to ArborDB. Please establish a connection first.");
            return;
        }

        clearScreen(20);

        try {
            // Set up the statement using user input
            int firstForest = 0;
            int secondForest = 0;
            try {
                System.out.println("Please input the forest_no of the starting forest.");
                firstForest = scanner.nextInt();
            } catch (InputMismatchException e) {
                System.out.println("Expected an integer, please try again.");
                return;
            } catch (NoSuchElementException e1){
                System.err.println("No lines were read from user input, please try again.");
                return;
            }

            try {
                System.out.println("Please input the forest_no of the second forest.");
                secondForest = scanner.nextInt();
            } catch (InputMismatchException e) {
                System.out.println("Expected an integer, please try again.");
                return;
            } catch (NoSuchElementException e1){
                System.err.println("No lines were read from user input, please try again.");
                return;
            }
            
            // prepare the statement to be executed
            CallableStatement preparedStatement = conn.prepareCall("SELECT * FROM threeDegrees( ?, ? )");
            preparedStatement.setInt(1, firstForest);
            preparedStatement.setInt(2, secondForest);
            boolean hasResults = preparedStatement.execute();
            if (hasResults){
                ResultSet resultSet = preparedStatement.getResultSet();
                resultSet.next();
                String hops = resultSet.getString("threedegrees");

                // Handle the case where no hops were found
                if (hops != null){
                    System.out.println("The following hops were found!\n" + hops);
                }else{
                    System.out.format("No hops were found from %d to %d", firstForest, secondForest);
                }
            }
        // Catch any SQL errors and display the error logs
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
        System.out.println("\nPlease enter your selection: \n");
    }

    public static void clearScreen(int n) {
        for (int i = 0; i < n; i++){
            System.out.println();
        }
    }
}