package ssa;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class DBConnect {
	private static Connection connection = null;
	public static Properties properties = null;
	
	
	static {
		properties = new Properties();
		try {
			properties.load(new FileInputStream("connection.properties"));			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static Connection getConnection() {
		String dbUrl = properties.getProperty("dburl");
		String user = properties.getProperty("user");
		String pass = properties.getProperty("password");
		try {
			connection = DriverManager.getConnection(dbUrl, user, pass);					
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return connection;
	}
	
	public static void close() {
		try {
			connection.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
