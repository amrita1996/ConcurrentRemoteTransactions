package org.example.jdbc;

public class SQLGenerator {
    public static String getCustomersWithZipCodeSQL(String zipcode) {
        return "SELECT customer_id FROM olist_customers_dataset " +
                "WHERE customer_zip_code_prefix = '" + zipcode + "'";
    }

    public static String updateCityForCustomersWithZipCodeSQL(String customerIds, String newCityValue) {
        return "UPDATE olist_customers_dataset\n" +
                "SET customer_city = '" + newCityValue+ "' " +
                "WHERE customer_id in " + customerIds;
    }
}
