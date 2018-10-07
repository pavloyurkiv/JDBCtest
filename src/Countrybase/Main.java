package Countrybase;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.InputMismatchException;
import java.util.Random;
import java.util.Scanner;

public class Main {

    static Connection conn;
    static Scanner sc = new Scanner(System.in);
    static Random random = new Random();

    public static void main(String[] args) throws SQLException {



        String dbUrl = "jdbc:mysql://localhost:3306/countrybase?useSSL=false&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"; // ексепшн з таймзоною
        String username = "root";
        String password = "12345";

        conn = DriverManager.getConnection(dbUrl, username, password);
        System.out.println("Connected? " + !conn.isClosed());

        createTablePerson();
        createTableCity();
        createTableCountry();
        connections();
        insertQueryCountryFromFile();
        insertQueryCountry();
        selectCountry();
        insertQueryCityFromFile();
        insertQueryCity();
        selectCity();
        insertQueryPersonFromFile();
        insertQueryPerson();
        selectPerson();
        selectPersonId();
        selectCityId();
        selectCountryId();
        selectPersonOnCity();
        selectCityOnCountry();
        selectPersonOnConsole();

        conn.close();

    }

    private static void createTablePerson() throws SQLException {
        String dropQuery = "DROP TABLE IF EXISTS person;";
        String query = "CREATE TABLE person("
                + "id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,"
                + "last_name VARCHAR(80) NOT NULL,"
                + "first_name VARCHAR(80) NOT NULL,"
                + "age INT NOT NULL,"
                + "city_id INT NOT NULL"
                + ");";

        Statement stmt = conn.createStatement();
        stmt.execute(dropQuery);
        stmt.execute(query);
        System.out.println("Table 'person' created!");
        stmt.close();
    }

    private static void createTableCity() throws SQLException {
        String dropQuery = "DROP TABLE IF EXISTS city;";
        String query = "CREATE TABLE city("
                + "id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,"
                + "name VARCHAR(80) NOT NULL,"
                + "country_id INT NOT NULL"
                + ");";

        Statement stmt = conn.createStatement();
        stmt.execute(dropQuery);
        stmt.execute(query);
        System.out.println("Table 'city' created!");
        stmt.close();
    }

    private static void createTableCountry() throws SQLException {
        String dropQuery = "DROP TABLE IF EXISTS country;";
        String query = "CREATE TABLE country("
                + "id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,"
                + "name VARCHAR(80) NOT NULL"
                + ");";

        Statement stmt = conn.createStatement();
        stmt.execute(dropQuery);
        stmt.execute(query);
        System.out.println("Table 'country' created!");
        stmt.close();
    }

    private static void connections() throws SQLException {
        String query = "ALTER TABLE city ADD FOREIGN KEY (country_id) REFERENCES country(id);";
        String query2 = "ALTER TABLE person ADD FOREIGN KEY (city_id) REFERENCES city(id);";

        Statement stmt = conn.createStatement();
        stmt.execute(query);
        stmt.execute(query2);
        System.out.println("Connections is done!");
        stmt.close();
    }

    private static void insertQueryCountryFromFile() throws SQLException {
        String query = "INSERT INTO country(name) VALUES(?);";
        PreparedStatement pstmt = conn.prepareStatement(query);

        try(BufferedReader br = new BufferedReader(new FileReader("countries.txt")))
        {

            String s;
            while((s=br.readLine())!=null){

                pstmt.setString(1, s);
                pstmt.executeUpdate();

            }
        }
        catch(IOException ex){

            System.out.println(ex.getMessage());
        }
        pstmt.close();
    }

    private static void insertQueryCityFromFile() throws SQLException {

        String query = "INSERT INTO city(name, country_id) VALUES(?, ?);";
        PreparedStatement pstmt = conn.prepareStatement(query);

        try(BufferedReader br = new BufferedReader(new FileReader("cities.txt")))
        {

            String s;
            while((s=br.readLine())!=null){

                pstmt.setString(1, s);
                pstmt.setInt(2, random.nextInt(21) + 1);
                pstmt.executeUpdate();

            }
        }
        catch(IOException ex){

            System.out.println(ex.getMessage());
        }

        pstmt.close();
    }

    private static void insertQueryPersonFromFile() throws SQLException {


        String query = "INSERT INTO person(last_name, first_name, age, city_id) VALUES(?, ?, ?, ?);";
        PreparedStatement pstmt = conn.prepareStatement(query);

        try(BufferedReader br = new BufferedReader(new FileReader("persons.txt")))
        {

            String s;
            while((s=br.readLine())!=null){


                String[] nm = s.split(" ");
                pstmt.setString(1, nm[0]);
                pstmt.setString(2, nm[1]);
                pstmt.setInt(3, random.nextInt(60) + 17);
                pstmt.setInt(4, random.nextInt(22) + 1);
                pstmt.executeUpdate();

            }
        }
        catch(IOException ex){

            System.out.println(ex.getMessage());
        }

        pstmt.close();
    }

    private static void insertQueryCountry() throws SQLException {
        System.out.println("Add country name:");
        String countryAdd = sc.nextLine();
        String query = "INSERT INTO country(name) VALUES(?);";

        PreparedStatement pstmt = conn.prepareStatement(query);
        pstmt.setString(1, countryAdd);
        pstmt.executeUpdate();
        pstmt.close();

    }

    private static void selectCountry() throws SQLException {
        String query = "SELECT * FROM country;";

        PreparedStatement pstmt = conn.prepareStatement(query);
        ResultSet rs = pstmt.executeQuery();

        while(rs.next()) {
            System.out.println(
                    "ID: " + rs.getInt("id") + "\t | " +
                            "Country name: " + rs.getString("name"));
        }
    }

    private static void insertQueryCity() throws SQLException {
        System.out.println("Add city name:");
        String cityAdd = sc.nextLine();
        String query = "INSERT INTO city(name, country_id) VALUES(?, ?);";

        PreparedStatement pstmt = conn.prepareStatement(query);
        pstmt.setString(1, cityAdd);
        pstmt.setInt(2, 1);

        pstmt.executeUpdate();
        pstmt.close();
    }

    private static void selectCity() throws SQLException {
        String query = "SELECT c.id, c.name, ct.name FROM city c JOIN country ct ON ct.id = c.country_id ORDER BY c.name DESC;";

        PreparedStatement pstmt = conn.prepareStatement(query);
        ResultSet rs = pstmt.executeQuery();

        while(rs.next()) {
            System.out.println(
                    "ID: " + rs.getInt("c.id") + "\t | " +
                            "City name: " + rs.getString("c.name") + "\t | " +
                            "Country name: " + rs.getString("ct.name"));
        }
    }

    private static void insertQueryPerson() throws SQLException {

        System.out.println("Add person lastname:");
        String lnameAdd = sc.nextLine();
        System.out.println("Add person name:");
        String nameAdd = sc.nextLine();
        System.out.println("Add person age:");
        int ageAdd = sc.nextInt();
        String query = "INSERT INTO person(last_name, first_name, age, city_id) VALUES(?, ?, ?, ?);";

        PreparedStatement pstmt = conn.prepareStatement(query);
        pstmt.setString(1, lnameAdd);
        pstmt.setString(2, nameAdd);
        pstmt.setInt(3, ageAdd);
        pstmt.setInt(4, 1);

        pstmt.executeUpdate();
        pstmt.close();
    }

    private static void selectPerson() throws SQLException {
        String query = "SELECT p.id, p.last_name, p.first_name, p.age, c.name, ct.name FROM person p " +
                "JOIN city c ON  p.city_id = c.id JOIN country ct ON ct.id = c.country_id ORDER BY p.last_name;";

        PreparedStatement pstmt = conn.prepareStatement(query);
        ResultSet rs = pstmt.executeQuery();

        while(rs.next()) {
            System.out.println(
                    "ID: " + rs.getInt("p.id") + "\t | " +
                            "Last name: " + rs.getString("p.last_name") + "\t | " +
                            "Name: " + rs.getString("p.first_name") + "\t | " +
                            "Age: " + rs.getString("p.age") + "\t | " +
                            "City name: " + rs.getString("c.name") + "\t | " +
                            "Country name: " + rs.getString("ct.name"));
        }
    }

    private static void selectPersonId() throws SQLException {
        System.out.println("Type person id: ");
        int pid = sc.nextInt();
        String query = "SELECT p.id, p.last_name, p.first_name, p.age FROM person p WHERE p.id = " + pid +
                ";";

        PreparedStatement pstmt = conn.prepareStatement(query);
        ResultSet rs = pstmt.executeQuery();

        while(rs.next()) {
            System.out.println(
                    "ID: " + rs.getInt("p.id") + "\t | " +
                            "Last name: " + rs.getString("p.last_name") + "\t | " +
                            "Name: " + rs.getString("p.first_name") + "\t | " +
                            "Age: " + rs.getString("p.age"));
        }
    }

    private static void selectCityId() throws SQLException {
        System.out.println("Type city id: ");
        int pid = sc.nextInt();
        String query = "SELECT c.id, c.name FROM city c WHERE c.id = " + pid +
                ";";

        PreparedStatement pstmt = conn.prepareStatement(query);
        ResultSet rs = pstmt.executeQuery();

        while(rs.next()) {
            System.out.println(
                    "ID: " + rs.getInt("c.id") + "\t | " +
                            "City name: " + rs.getString("c.name"));
        }
    }

    private static void selectCountryId() throws SQLException {
        System.out.println("Type country id: ");
        int pid = sc.nextInt();
        String query = "SELECT ct.id, ct.name FROM country ct WHERE ct.id = " + pid +
                ";";

        PreparedStatement pstmt = conn.prepareStatement(query);
        ResultSet rs = pstmt.executeQuery();

        while(rs.next()) {
            System.out.println(
                    "ID: " + rs.getInt("id") + "\t | " +
                            "Country name: " + rs.getString("name"));
        }
    }

    private static void selectPersonOnCity() throws SQLException {
        System.out.println("Select Person On City - Type city id: ");
        int pid = sc.nextInt();
        String query = "SELECT p.id, p.last_name, p.first_name, p.age, cy.name, ct.name FROM person p" +
                " JOIN city cy ON  p.city_id = cy.id JOIN country ct ON ct.id = cy.country_id WHERE p.city_id = " + pid + ";";

        PreparedStatement pstmt = conn.prepareStatement(query);
        ResultSet rs = pstmt.executeQuery();

        while(rs.next()) {
            System.out.println(
                    "ID: " + rs.getInt("p.id") + "\t | " +
                            "Last name: " + rs.getString("p.last_name") + "\t | " +
                            "Name: " + rs.getString("p.first_name") + "\t | " +
                            "Age: " + rs.getString("p.age") + "\t | " +
                            "City name: " + rs.getString("cy.name") + "\t | " +
                            "Country name: " + rs.getString("ct.name"));
        }
    }

    private static void selectCityOnCountry() throws SQLException {

        System.out.println("Select City On Country - Type country id: ");
        int pid = sc.nextInt();
        String query = "SELECT c.id, c.name, ct.name FROM city c JOIN country ct ON ct.id = c.country_id WHERE ct.id = " + pid + " ORDER BY c.name;";

        PreparedStatement pstmt = conn.prepareStatement(query);
        ResultSet rs = pstmt.executeQuery();

        while(rs.next()) {
            System.out.println(
                    "ID: " + rs.getInt("c.id") + "\t | " +
                            "City name: " + rs.getString("c.name") + "\t | " +
                            "Country name: " + rs.getString("ct.name"));
        }
    }

    private static void selectPersonOnConsole() throws SQLException {
        System.out.println("Select Person On Console - Type first name:");
        String name = sc.nextLine(); // цей рядок чомусь ігнорується. Чому?
        String query = "SELECT p.id, p.last_name, p.first_name, p.age, cy.name, ct.name FROM person p" +
                    " JOIN city cy ON  p.city_id = cy.id JOIN country ct ON ct.id = cy.country_id WHERE p.first_name LIKE '%" + name + "%';";


        PreparedStatement pstmt = conn.prepareStatement(query);
        ResultSet rs = pstmt.executeQuery();

        while(rs.next()) {
            System.out.println(
                    "ID: " + rs.getInt("p.id") + "\t | " +
                            "Last name: " + rs.getString("p.last_name") + "\t | " +
                            "Name: " + rs.getString("p.first_name") + "\t | " +
                            "Age: " + rs.getString("p.age") + "\t | " +
                            "City name: " + rs.getString("cy.name") + "\t | " +
                            "Country name: " + rs.getString("ct.name"));
        }
    }
}
