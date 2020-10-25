import com.mongodb.DBObject;
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
        connect();
    }

    public static void connect() {
        Connection conn = null;
        //Statement stmt = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection(Variables.getConnString(), Variables.getUSERNAME(), Variables.getPASSWORD());
            System.out.println("Connected to MySQL");

            exportFromMySQLToMongoDB(conn);

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
    public static void exportFromMySQLToMongoDB(Connection conn) {
        //Statement stmt;
        String query;
        int offset = 0;

        MongoClient mongoClient = new MongoClient("localhost", 27017);
        MongoDatabase database = mongoClient.getDatabase("diplomka");
        MongoCollection<Document> collection = database.getCollection("gloffer_cache");

        System.out.println("Connected to MongoDB");

        Date currentDate = new Date();
        System.out.println(currentDate.toString() + " - Starting...");

        while (true) {
            try {
                Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                        ResultSet.CONCUR_UPDATABLE);

                query = "SELECT cache as cache FROM " + Variables.getTableName() + " LIMIT " + Variables.getCOUNT() + " OFFSET " + offset;
                //System.out.println(query);
                offset += Variables.getCOUNT();
                ResultSet rs = stmt.executeQuery(query);

                if (!rs.first()) {
                    break;
                }

                insertResultSetInMongo(rs, collection);

                if (offset % 100000 == 0) {
                    currentDate = new Date();
                    System.out.println(currentDate.toString() + " - 100k");
                }

                stmt.close();


            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void insertResultSetInMongo(ResultSet rs, MongoCollection<Document> collection) {
        while (true) {
            try {
                if (!rs.next()) break;
                String cache = rs.getString("cache");
                //System.out.println("Inserting: " + cache);
                Document document = Document.parse(cache);
                collection.insertOne(document);

            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }


        }

    };



}
