package com.pandatv.tools;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author: likaiqing
 * @create: 2018-10-15 12:32
 **/
public class MysqlPool {
    private static final Logger logger = LogManager.getLogger(MysqlPool.class);

    private static BasicDataSource bs = null;

    public static void main(String[] args) {
        ExecutorService service = Executors.newFixedThreadPool(60);
        for (int i = 0; i < 60; i++) {
            service.submit(new Runnable() {
                @Override
                public void run() {
                    Connection connection = getConnection();
                    PreparedStatement ps = null;
                    ResultSet rs = null;
                    try {
                        ps = connection.prepareStatement("select name from job where id=1");
                        rs = ps.executeQuery();
                        while (rs.next()) {
                            System.out.println(rs.getString(1));
                        }
                        closeConn(rs, ps, connection);
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            });
        }
        service.shutdown();
    }

    /**
     * 创建数据源
     */
    public static BasicDataSource getDataSource() {
        if (bs == null) {
            synchronized (MysqlPool.class) {
                if (bs == null) {
                    bs = new BasicDataSource();
                    bs.setDriverClassName("com.mysql.jdbc.Driver");
                    bs.setUrl("jdbc:mysql://10.131.9.168:3306/villa");
                    bs.setUsername("analysis1");
                    bs.setPassword("Kdg0XFM6ixh70jZWC");
//                    bs.setUrl("jdbc:mysql://localhost:3306/test");
//                    bs.setUsername("likaiqing");
//                    bs.setPassword("");
                    bs.setMaxActive(200);//最大并发数
                    bs.setInitialSize(10);//数据库初始化时，创建的连接个数
                    bs.setMinIdle(5);//最小空闲连接数
                    bs.setMaxIdle(200);//数据库最大连接数
                    bs.setMaxWait(1000);
                    bs.setMinEvictableIdleTimeMillis(60 * 1000);//空闲连接60秒中后释放
                    bs.setTimeBetweenEvictionRunsMillis(5 * 60 * 1000);//5分钟检测一次释放有死掉的线程
                    bs.setTestOnBorrow(true);
                }
            }

        }
        return bs;
    }

    /**
     * 创建数据源
     */
    public static BasicDataSource getDataSource(String url, String name, String pwd) {
        if (bs == null) {
            synchronized (MysqlPool.class) {
                if (bs == null) {
                    bs = new BasicDataSource();
                    bs.setDriverClassName("com.mysql.jdbc.Driver");
                    bs.setUrl(url);
                    bs.setUsername(name);
                    bs.setPassword(pwd);
//                    bs.setUrl("jdbc:mysql://localhost:3306/test");
//                    bs.setUsername("likaiqing");
//                    bs.setPassword("");
                    bs.setMaxActive(200);//最大并发数
                    bs.setInitialSize(10);//数据库初始化时，创建的连接个数
                    bs.setMinIdle(5);//最小空闲连接数
                    bs.setMaxIdle(200);//数据库最大连接数
                    bs.setMaxWait(1000);
                    bs.setMinEvictableIdleTimeMillis(60 * 1000);//空闲连接60秒中后释放
                    bs.setTimeBetweenEvictionRunsMillis(5 * 60 * 1000);//5分钟检测一次释放有死掉的线程
                    bs.setTestOnBorrow(true);
                }
            }

        }
        return bs;
    }

    /**
     * 释放数据源
     */
    public static void shutDownDataSource() throws SQLException {
        if (bs != null) {
            bs.close();
        }
    }

    /**
     * 获取数据库连接
     */
    public static Connection getConnection(String url, String name, String pwd) {
        Connection conn = null;
        try {
            if (bs != null) {
                conn = bs.getConnection();
            } else {
                conn = getDataSource(url, name, pwd).getConnection();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }

    /**
     * 获取数据库连接
     */
    public static Connection getConnection() {
        Connection conn = null;
        try {
            if (bs != null) {
                conn = bs.getConnection();
            } else {
                conn = getDataSource().getConnection();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }

    /**
     * 关闭连接
     */
    public static void closeConn(ResultSet rs, PreparedStatement ps, Connection con) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
                logger.error("关闭结果集ResultSet异常!" + e.getMessage(), e);
            }
        }
        if (ps != null) {
            try {
                ps.close();
            } catch (Exception e) {
                logger.error("预编译SQL语句对象PreparedStatement关闭异常！" + e.getMessage(), e);
            }
        }
        if (con != null) {
            try {
                con.close();
            } catch (Exception e) {
                logger.error("关闭连接对象Connection异常！" + e.getMessage(), e);
            }
        }
    }

}
