package com.db;

import java.sql.Connection;

public abstract class DBConnectionPool
{
    public abstract Connection getConnection() throws Exception;

    public abstract void putBackConnection(Connection conn) throws Exception;
}