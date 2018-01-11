package com.cheney.dbcp.mtp.pool;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import com.cheney.dbcp.mtp.utils.CheneyUtils;

public final class CheneyConnectionPool {

	private static List<Connection> connections;
	private static ReentrantLock lock = new ReentrantLock();
	
	static {
		lock.lock();
		try {
			int size = Integer.valueOf(CheneyUtils.getStringValue("connectionPoolSize"));
			connections = new LinkedList<Connection>();
			try {
				Class.forName(CheneyUtils.getStringValue("driver"));
				for (int i = 0; i < size; i++)
					connections.add(DriverManager.getConnection(CheneyUtils.getStringValue("url"), CheneyUtils.getStringValue("user"), CheneyUtils.getStringValue("password")));
			} catch (ClassNotFoundException | SQLException e) {
				CheneyUtils.exception(e);
			}
		} finally {
			lock.unlock();
		}
	}
	
	public static Connection getConnection() {
		lock.lock();
		try {
			return connections.remove(0);
		} finally {
			lock.unlock();
		}
	}
	
	public static void close(Connection con) {
		lock.lock();
		try {
			connections.add(con);
		} finally {
			lock.unlock();
		}
	}
	
	public static void close(Connection con, ResultSet rs) {
		close(con);
		try {
			if (rs != null) {
				rs.close();
				rs = null;
			}
		} catch (SQLException e) {
			CheneyUtils.exception(e);
		}
	}
	
	public static void close(Connection con, ResultSet rs, Statement stmt) {
		close(con);
		try {
			if (rs != null) {
				rs.close();
				rs = null;
			}
			if (stmt != null) {
				stmt.close();
				stmt = null;
			}
		} catch (SQLException e) {
			CheneyUtils.exception(e);
		}
	}
	
	public static void release() {
		lock.lock();
		try {
			while (!connections.isEmpty())
				try {
					Connection con = getConnection();
					con.close();
					con = null;
				} catch (SQLException e) {
					CheneyUtils.exception(e);
				}
		} finally {
			lock.unlock();
		}
	}

}
