package org.example.jdbc;

import java.util.List;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import static java.util.logging.Level.INFO;

public class TransactionManager {
    public static final String T_1_CITY = "T1 City";
    public static final String T_2_CITY = "T2 City";
    public static final String T_3_CITY = "T3 City";
    public static final String ZIP_CODE = "28970";
    public static final String DB_LOG = "assignment-2/src/main/resources/db.log";
    public static final boolean APPEND = true;
    public static final int MILLIS = 1000;
    private JDBCConnector jdbcConnector;
    private Logger logger;

    public TransactionManager() {
        try {
            jdbcConnector = new JDBCConnector();
            FileHandler fileHandler = new FileHandler(DB_LOG, APPEND);
            fileHandler.setFormatter(new SimpleFormatter());
            logger = Logger.getLogger(TransactionManager.class.getName());
            logger.addHandler(fileHandler);

        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }

    public void executeWithoutSynchronization() {
        logger.log(INFO, "Execute Without Synchronization");
        getUnSynchronizedThread(this::transactionT1).start();
        getUnSynchronizedThread(this::transactionT2).start();
        getUnSynchronizedThread(this::transactionT3).start();
    }

    private Thread getUnSynchronizedThread(Runnable method) {
        return new Thread(() -> {
            try {
                method.run();
            } catch (Exception exception) {
                exception.printStackTrace();
            }
        });
    }

    public void executeWithSynchronization() {
        logger.log(INFO, "Execute With Synchronization");
        getSynchronizedThread(this::transactionT1).start();
        getSynchronizedThread(this::transactionT2).start();
        getSynchronizedThread(this::transactionT3).start();
    }

    private Thread getSynchronizedThread(Runnable method) {
        return new Thread(() -> {
            synchronized (jdbcConnector) {
                    try {
                        method.run();
                    } catch (Exception exception) {
                        exception.printStackTrace();
                    }
            }
        });
    }

    private void transactionT1() {
        try {
            logger.log(INFO, "T1 get customers");
            List<String> customersWithZipCode = jdbcConnector.getCustomersWithZipCode(ZIP_CODE);
            logger.log(INFO, "T1 update customers");
            jdbcConnector.updateCity(customersWithZipCode, T_1_CITY);
            Thread.sleep(MILLIS);
            logger.log(INFO, "T1 commit");
            jdbcConnector.commit();
        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }

    private void transactionT2() {
        try {
            logger.log(INFO, "T2 get customers");
            List<String> customersWithZipCode = jdbcConnector.getCustomersWithZipCode(ZIP_CODE);
            Thread.sleep(MILLIS);
            logger.log(INFO, "T2 update customers");
            jdbcConnector.updateCity(customersWithZipCode, T_2_CITY);
            Thread.sleep(MILLIS);
            logger.log(INFO, "T2 commit");
            jdbcConnector.commit();
        } catch (Exception exception) {
            exception.printStackTrace();
        }

    }

    private void transactionT3() {
        try {
            Thread.sleep(MILLIS);
            logger.log(INFO, "T3 get customers");
            List<String> customersWithZipCode = jdbcConnector.getCustomersWithZipCode(ZIP_CODE);
            logger.log(INFO, "T3 update customers");
            jdbcConnector.updateCity(customersWithZipCode, T_3_CITY);
            logger.log(INFO, "T3 commit");
            jdbcConnector.commit();
        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }
}
