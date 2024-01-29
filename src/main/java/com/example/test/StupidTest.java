package com.example.test;

import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.github.f4b6a3.uuid.UuidCreator;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

public class StupidTest {

    private static final Gson GSON = new GsonBuilder().serializeNulls().create();
    private static final Type GSON_EVENTS_TYPE = new TypeToken<Map<String, Object>>() {}.getType();
    private static final String ID = DateTimeFormatter.ofPattern("yyyyMMddHHmmss").format(LocalDateTime.now());
    private static final Random RANDOM = new Random();
    
    private void start() {
        createNewDbAndSchemas();
        ExecutorService es = prepareExecutorService();
        produceAndSinkEvents(es);
        //consumeEventAndProcess(es);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                System.out.println("shutting down cleanly");
                es.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }));
    }

    private void consumeEventAndProcess(ExecutorService es) {
        ArrayBlockingQueue<Map<String, Object>> queue = new ArrayBlockingQueue<>(100_000, false);
        es.submit(() -> queryEventsForProcessingQueue(queue));
        es.submit(() -> processEvents(queue));
    }

    private void processEvents(ArrayBlockingQueue<Map<String, Object>> queue) {
        Instant offset = Instant.now();
        try (Connection connection = getSqliteConnection()) {
            connection.setAutoCommit(false);
            for (int count = 0; !Thread.interrupted(); count++) {
                Map<String, Object> event = queue.take();
                processEvent(connection, event);
                if (isThresholdInSecondsReached(offset, 1)) {
                    connection.commit();
                    logMetric("processEvents", count, 1);
                    offset = Instant.now();
                    count = -1;
                }
            }
            if (Thread.interrupted()) {
                System.out.println("processEvent interrupted, committing");
                connection.commit();
            }
        } catch (SQLException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void queryEventsForProcessingQueue(ArrayBlockingQueue<Map<String, Object>> queue) {
        try (Connection connection = getSqliteConnection()) {
            Instant offset = Instant.now();
            Optional<String> dbOffset = getLatestOffsetFromDb(connection);
            for (int count = 0; !Thread.interrupted(); count++) {
                List<Map<String, Object>> events = queryEvents(connection, dbOffset, 200);
                if (events.isEmpty()) {
                    Thread.sleep(1000);
                    continue;
                }
                for (Map<String, Object> event: events) {
                    while (!queue.offer(event)) {
                        Thread.sleep(200);
                    }
                    dbOffset = Optional.of((String) event.get("id"));
                }
                if (isThresholdInSecondsReached(offset, 5)) {
                    logMetric("queryEventsForProcessingQueue", count, 5);
                    offset = Instant.now();
                    count = -1;
                }
            }
            if (Thread.interrupted()) {
                System.out.println("queryEventsForProcessingQueue interrupted");
            }
        } catch (SQLException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private List<Map<String, Object>> queryEvents(Connection connection, Optional<String> offset, int limit) throws SQLException {
        ResultSet rs = connection.prepareStatement($("select id, json from events %s limit %s",
                offset.isPresent() ? $("where id > '%s'", offset.get()) : ""),
                limit)
            .executeQuery();
        List<Map<String, Object>> events = new ArrayList<>();
        while (rs.next()) {
            events.add(GSON.fromJson(rs.getString("json"), GSON_EVENTS_TYPE));
        }
        return events;
    }

    private Optional<String> getLatestOffsetFromDb(Connection connection) throws SQLException {
        ResultSet rs = connection.prepareStatement("select offset from offsets where name = 'main'")
            .executeQuery();
        return rs.next() ? Optional.ofNullable(rs.getString("offset")) : Optional.empty();
    }

    private void produceAndSinkEvents(ExecutorService es) {
        ArrayBlockingQueue<Map<String, Object>> queue = new ArrayBlockingQueue<>(100_000, false);
        es.submit(() -> produceEvents(queue));
        es.submit(() -> sinkEventsToDb(queue));
    }

    private void sinkEventsToDb(ArrayBlockingQueue<Map<String, Object>> queue) {
        System.out.println("sinkEventsToDb started");
        Instant offset = Instant.now();
        int thresholdSec = 1;
        try (Connection connection = getSqliteConnection()) {
            connection.setAutoCommit(false);
            for (int count = 0; !Thread.interrupted(); count++) {
                Map<String, Object> event = queue.take();
                sinkEvent2db(connection, event);
                if (isThresholdInSecondsReached(offset, thresholdSec)) {
                    connection.commit();
                    logMetric("sinkEventsToDb", count, thresholdSec);
                    offset = Instant.now();
                    count = -1;
                }
            }
            if (Thread.interrupted()) {
                System.out.println("sinkEventsToDb interrupted, committing");
                connection.commit();
            }
        } catch (SQLException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void sinkEvent2db(Connection connection, Map<String, Object> event) throws SQLException {
        PreparedStatement statement = connection.prepareStatement("insert into events (id, json) values (?, ?)");
        statement.setString(1, uuid7());
        statement.setString(2, GSON.toJson(event));
        statement.executeUpdate();
    }

    private String uuid7() {
        return UuidCreator.getTimeOrderedEpoch().toString();
    }

    private void processEvent(Connection connection, Map<String, Object> event) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(
                $("insert into %s (id, name, age, single) values (?, ?, ?, ?)", event.get("type")));
        statement.setString(1, uuid7());
        statement.setString(2, (String) event.get("name"));
        statement.setInt(3, (Integer) event.get("age"));
        statement.setBoolean(4, (Boolean) event.get("single"));
        statement.executeUpdate();
    }

    private void produceEvents(ArrayBlockingQueue<Map<String, Object>> queue) {
        System.out.println("produceEvents started");
        Instant offset = Instant.now();
        int thresholdSec = 1;
        for (int count = 0; !Thread.interrupted(); count++) {
            queue.add(getRandomEvent());
            if (count % 20000 == 0) {
                try {
                    Thread.sleep((RANDOM.nextInt(2) + 1) * 100);
                } catch(Exception e) {
                    e.printStackTrace();
                }
            }
            if (isThresholdInSecondsReached(offset, thresholdSec)) {
                logMetric("produceEvents", count, thresholdSec);
                offset = Instant.now();
                count = -1;
            }
        }
        if (Thread.interrupted()) {
            System.out.println("produceEvents interrupted");
        }
    }

    private void logMetric(String eventType, int count, int elapsedSecs) {
        System.out.printf("%s, %s/%s\n", 
                eventType, 
                count, $("%s", elapsedSecs == 1 ? "sec" : elapsedSecs + "secs"));
    }

    private boolean isThresholdInSecondsReached(Instant offset, int secs) {
        return Duration.between(offset, Instant.now()).toSeconds() >= secs;
    }

    private Map<String, Object> getRandomEvent() {
        Map<String, Object> map = new HashMap<>();
        map.put("id", uuid7());
        map.put("type", $("person%s", RANDOM.nextInt(3) + 1));
        map.put("name", $("albert_gan-%s", RANDOM.nextInt(1_000_000) + 1));
        map.put("age", RANDOM.nextInt(100) + 1);
        map.put("single", RANDOM.nextBoolean());
        return map;
    }

    private ExecutorService prepareExecutorService() {
        return Executors.newFixedThreadPool(4);
    }

    private void createNewDbAndSchemas() {
        try (Connection connection = getSqliteConnection()) {
            Statement statement = connection.createStatement();

            statement.executeUpdate("drop table if exists events");
            statement.executeUpdate("create table events (id string, json string)");
            
            statement.executeUpdate("drop table if exists offsets");
            statement.executeUpdate("create table offsets (id string, name string, offset string)");
            statement.executeUpdate($("insert into offsets (id, name , offset) values ('%s', 'main', NULL)", uuid7()));
            
            statement.executeUpdate("drop table if exists person1");
            statement.executeUpdate("create table person1 (id string, name string, age integer, single boolean)");
            
            statement.executeUpdate("drop table if exists person2");
            statement.executeUpdate("create table person2 (id string, name string, age integer, single boolean)");
            
            statement.executeUpdate("drop table if exists person3");
            statement.executeUpdate("create table person3 (id string, name string, age integer, single boolean)");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private Connection getSqliteConnection() throws SQLException {
        return DriverManager.getConnection($("jdbc:sqlite:sample-%s.db", ID));
    }

    public static void main(String[] args) {
        new StupidTest().start();
    }

    private static String $(String template, Object...args) {
        return String.format(template, args);
    }
}
