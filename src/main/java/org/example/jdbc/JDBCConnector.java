package org.example.jdbc;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static java.util.Collections.emptyList;
import static org.example.jdbc.SQLGenerator.getCustomersWithZipCodeSQL;
import static org.example.jdbc.SQLGenerator.updateCityForCustomersWithZipCodeSQL;

public class JDBCConnector {
    public static final String URL = "url";
    public static final String USER = "user";
    public static final String PASSWORD = "password";
    public static final String CUSTOMER_ID = "customer_id";
    public static final String SRC_MAIN_RESOURCES_DB_PROPERTIES = "assignment-2/src/main/resources/db.properties";

    private final Connection connection;

    public JDBCConnector() throws SQLException, IOException {
        this.connection = getConnection();
    }

    public void updateCity(List<String> customersWithZipCode, String newCityValue) {
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(
                    updateCityForCustomersWithZipCodeSQL(getFormattedString(customersWithZipCode), newCityValue));
            preparedStatement.executeUpdate();
        } catch (SQLException exception) {
            exception.printStackTrace();
        }
    }

    public List<String> getCustomersWithZipCode(String zipCodePrefix) {
        PreparedStatement preparedStatement;
        ResultSet resultSet;
        try {
            preparedStatement = connection.prepareStatement(getCustomersWithZipCodeSQL(zipCodePrefix));
            resultSet = preparedStatement.executeQuery();
            List<String> customerIds = new ArrayList<>();
            while (resultSet.next()) {
                customerIds.add(resultSet.getString(CUSTOMER_ID));
            }
            return customerIds;
        } catch (SQLException exception) {
            exception.printStackTrace();
        }
        return emptyList();
    }

    public void commit() {
        try {
            connection.commit();
        } catch (SQLException exception) {
            exception.printStackTrace();
        }
    }

    private String getFormattedString(List<String> customersWithZipCode) {
        String formattedString = "(";
        for (int i = 0; i < customersWithZipCode.size(); i++) {
            formattedString = formattedString.concat("'");
            formattedString = formattedString.concat(customersWithZipCode.get(i));
            if(i == customersWithZipCode.size() - 1) {
                formattedString = formattedString.concat("'");
            } else {
                formattedString = formattedString.concat("', ");
            }
        }
        formattedString = formattedString.concat(")");
        return formattedString;
    }

    private Connection getConnection() throws IOException, SQLException {
            Properties properties = getProperties();
        Connection connection = DriverManager.getConnection(properties.getProperty(URL), properties.getProperty(USER),
                properties.getProperty(PASSWORD));
        connection.setAutoCommit(false);
        return connection;
    }

    private Properties getProperties() throws IOException {
        FileInputStream fileInputStream = new FileInputStream(SRC_MAIN_RESOURCES_DB_PROPERTIES);
        Properties properties = new Properties();
        properties.load(fileInputStream);
        return properties;
    }
}
