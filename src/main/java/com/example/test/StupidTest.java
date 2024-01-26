package com.example.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ExecutorService;

public class StupidTest {

    private void start() {
        createNewDbAndSchemas();
        ExecutorService es = prepareExecutorService();
        produceAndSinkEvents(es);
        consumeEventAndProcess(es);
    }

    private void createNewDbAndSchemas() {
        Connection connection = null;
        try {
            connection = DriverManager.getConnection($("jdbc:sqlite:%s_sample.db", whenString()));
            Statement statement = connection.createStatement();
            statement.setQueryTimeout(30);

            statement.executeUpdate("drop table if exists person");
            statement.executeUpdate("create table person (id string, name string)");
            
            statement.executeUpdate("drop table if exists person");
            statement.executeUpdate("create table person2 (id string, name string)");
            
            statement.executeUpdate("drop table if exists person");
            statement.executeUpdate("create table person3 (id string, name string)");
            
            // UuidCreator.getTimeOrderedEpoch()
            
            /*ResultSet rs = statement.executeQuery("select * from person");
            while (rs.next()) {
                // read the result set
                System.out.println("name = " + rs.getString("name"));
                System.out.println("id = " + rs.getInt("id"));
            }*/
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if (connection != null)
                    connection.close();
            } catch (SQLException e) {
                System.err.println(e.getMessage());
                e.printStackTrace();
            }
        }
    }

    private String whenString() {
        return DateTimeFormatter.ofPattern("yyyy-MM-dd").format(LocalDate.now());
    }

    public static void main(String[] args) {
        new StupidTest().start();
    }

    private static String $(String template, Object...args) {
        return String.format(template, args);
    }
}
