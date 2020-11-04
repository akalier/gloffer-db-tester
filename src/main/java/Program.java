import com.mongodb.DBObject;
import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.util.JSON;
import org.json.JSONObject;
import org.bson.Document;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Date;

public class Program {

    public static void main(String[] args) {
        int firstArg, secondArg;
        if (args.length > 0) {
            try {
                firstArg = Integer.parseInt(args[0]);
                secondArg = Integer.parseInt(args[1]);
                connect(firstArg, secondArg);
            } catch (NumberFormatException e) {
                System.err.println("Argument" + args[0] + " must be an integer.");
                System.exit(1);
            }
        } else {
            connect(10000000, 0);
        }
        //connect();
    }

    public static void connect(int totalLimit, int offset) {
        Connection conn = null;
        //Statement stmt = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection(Variables.getConnString(), Variables.getUSERNAME(), Variables.getPASSWORD());
            System.out.println("Connected to MySQL");

            exportFromMySQLToMongoDB(conn, totalLimit, offset);

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
    public static void exportFromMySQLToMongoDB(Connection conn, int totalLimit, int offset) {
        //Statement stmt;
        String query;
        //int offset = 0;
        //System.out.println("offset: " + offset);
        int counter = offset;
        totalLimit += offset;
        //System.out.println("counter: " + counter);

        MongoClient mongoClient = new MongoClient("localhost", 27017);
        MongoDatabase database = mongoClient.getDatabase("diplomka");
        MongoCollection<Document> collection = database.getCollection("gloffer_cache");

        System.out.println("Connected to MongoDB");

        Date currentDate = new Date();
        System.out.println(currentDate.toString() + " - Starting...");
        Statement stmt;
        ResultSet rs;

        while (true) {
            try {
                stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                        ResultSet.CONCUR_UPDATABLE);

                int queryLimit = Variables.getCOUNT() + 1;
                query = "SELECT cache as cache FROM " + Variables.getTableName() + " WHERE id >= " + counter + " ORDER BY id LIMIT " + queryLimit;
                System.out.println(query);
                //offset += Variables.getCOUNT();
                //System.out.println("offset: " + offset);
                System.out.println("Executing query...");
                rs = stmt.executeQuery(query);

                if (!rs.first()) {
                    System.out.println("No more records, quitting...");
                    break;
                }

                counter += Variables.getCOUNT();

                //mongoDB
                insertResultSetInMongo(rs, collection);

                //elastic
                insertResultSetInElastic(rs);

                /*if (offset % 100000 == 0) {
                    currentDate = new Date();
                    System.out.println(currentDate.toString() + " - 100k");
                }*/

                if (counter >= totalLimit) {
                    currentDate = new Date();
                    System.out.println(currentDate.toString() + " - Ending...");
                    System.out.println("Inserted " + counter + " rows.");
                    break;
                }

                stmt.close();
                rs.close();


            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void insertResultSetInMongo(ResultSet rs, MongoCollection<Document> collection) {
        List<Document> documents = new ArrayList<>();

        System.out.println("Creating list...");
        try {
            while (true) {
                try {
                    if (!rs.next()) break;
                    String cache = rs.getString("cache");
                    //System.out.println("Inserting: " + cache);
                    Document document = Document.parse(cache);
                    documents.add(document);

                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }

            }
            //System.out.println("Inserting to collection...")
            collection.insertMany(documents);
            System.out.println("Inserted " + documents.size() + "\n________");
        }
        catch (Exception ex)
        {
            System.out.println(ex.toString());
        }


    };

    public static void insertResultSetInElastic(ResultSet rs) {

    }



}
