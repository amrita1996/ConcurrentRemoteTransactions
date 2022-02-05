package org.example;

import org.example.jdbc.TransactionManager;

public class Application {
    public static void main(String[] args) {
        TransactionManager transactionManager = new TransactionManager();
        if(args.length > 0 && args[0].equals("lock"))
        {
            transactionManager.executeWithSynchronization();

        }
        if(args.length > 0 && args[0].equals("nolock"))
        {
            transactionManager.executeWithoutSynchronization();

        }


    }
}
