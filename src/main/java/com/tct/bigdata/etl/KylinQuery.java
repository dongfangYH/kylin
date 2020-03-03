/*
package com.tct.bigdata.etl;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

*/
/**
 * @author yuanhang.liu@tcl.com
 * @description
 * @date 2020-02-24 14:30
 **//*

public class KylinQuery {

    private static final String SQL_TEMPLATE = "select count(distinct teyeid) from kylin_mobile_event where info >= ? " +
            "and info <= ? and packagename = ?";

    public static void main(String[] args) throws Exception{
        Driver driver = (Driver) Class.forName("org.apache.kylin.jdbc.Driver").newInstance();
        Properties info = new Properties();
        info.put("user", "ADMIN");
        info.put("password", "KYLIN");
        Connection conn = driver.connect("jdbc:kylin://10.90.18.7:7070/teye", info);
        PreparedStatement state = conn.prepareStatement(SQL_TEMPLATE);
        state.setString(1, "2020-01-25");
        state.setString(2, "2020-02-20");
        state.setString(3, "com.tclhz.gallery");
        ResultSet resultSet = state.executeQuery();

        if (resultSet.next()) {
            Long count = resultSet.getLong(1);
            System.out.println("result : " + count);
        }
        resultSet.close();
        state.close();
        conn.close();
    }
}
*/
