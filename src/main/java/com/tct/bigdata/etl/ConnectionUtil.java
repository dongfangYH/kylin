package com.tct.bigdata.etl;

import com.facebook.presto.jdbc.PrestoDriver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class ConnectionUtil {

    public static final String HIVE_DRIVER = "org.apache.hive.jdbc.HiveDriver";

    /**
     * get hive connection
     * @param url eg: jdbc:hive2://127.0.0.1:10000/default root null
     * @return
     * @throws Exception
     */
    public static Connection getHiveConn(String url, String user, String password) throws Exception {
        Class.forName(HIVE_DRIVER);
        return DriverManager.getConnection(url, user, password);
    }

    public static Connection getPrestoConn(String prestoUrl) throws SQLException {
        DriverManager.registerDriver(new PrestoDriver());
        Connection connection = DriverManager.getConnection(prestoUrl, new Properties());
        return connection;
    }
}
