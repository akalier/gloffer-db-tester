import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class Program {

    public static void main(String[] args) {
        connect();
    }

    public static void connect() {
        Connection conn = null;
        //Statement stmt = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection(Variables.getConnString(), Variables.getUSERNAME(), Variables.getPASSWORD());
            System.out.println("Connected");

            List<String> tables = getAllTables(conn);
            int limit = 10;
            int counter = 0;
            for (String table : tables) {
                counter++;
                exportDataCSV(conn, table, "D://diplomka/javadump/");
                if (counter > limit) break;
            }


            //STEP 6: Clean-up environment
            //stmt.close();
            conn.close();
        } catch (SQLException e) {
            System.err.println(e);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    //this approach does not work if we need to place the outfile on host machine (different than mysql server is running on)
    public static void exportDataCSVOnServerMachine(Connection conn, String tableName, String filePath) {
        System.out.println("Exporting table: " + tableName);
        Statement stmt;
        String query;
        try {
            stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);

            //For comma separated file
            query = "SELECT * into OUTFILE  '"+filePath+tableName+".csv" +
                    "' FIELDS TERMINATED BY ',' FROM " + tableName + " t";
            System.out.println("Executing query: " + query);
            stmt.executeQuery(query);

        } catch(Exception e) {
            e.printStackTrace();
            stmt = null;
        }
    }

    //this is not going to work either.
    public static void exportDataCSV(Connection conn, String tableName, String filePath) {
        System.out.println("Exporting table: " + tableName);
        String completePath = filePath + tableName + ".csv";
        String query = "SELECT * INTO OUTFILE '" + completePath + "' FIELDS TERMINATED BY ',' FROM " + tableName + " t";
        String command = "mysql -h " + Variables.getServerIp() + " -u " + Variables.getUSERNAME() + " -p" + Variables.getPASSWORD() + " " + Variables.getDbName() + " -e \"" + query + "\" > " + completePath;
        System.out.println(command);
        try {
            Process process = Runtime.getRuntime().exec(command);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    //get all table names from schema
    public static List<String> getAllTables(Connection conn) {
        Statement stmt = null;
        ArrayList<String> tables = new ArrayList<>();
        try {
            stmt = conn.createStatement();


            //get all tables with 0 or more rows from information schema
            String sql;
            sql = "SELECT table_name, table_rows\n" +
                    "    FROM INFORMATION_SCHEMA.TABLES\n" +
                    "    WHERE TABLE_SCHEMA = 'gloffer' AND table_rows >= 0\n" +
                    "    ORDER BY table_rows ASC;";
            ResultSet rs = stmt.executeQuery(sql);

            while (rs.next()) {
                //Retrieve by column name
                String table_name = rs.getString("table_name");
                int table_rows = rs.getInt("table_rows");

                //Display values
                //System.out.print("table_name: " + table_name + ", rows: " + table_rows + "\n");

                tables.add(table_name);
            }

            rs.close();
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return tables;

    }

}
