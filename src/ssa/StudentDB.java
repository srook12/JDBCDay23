package ssa;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class StudentDB {
	private static Connection connection = null;
	private static PreparedStatement pStatement = null;
	
	private static final String SELECT_STUDENTS = "select * from student";
	
	public StudentDB() {
		connection = DBConnect.getConnection();
	}

	public String printStudents() throws SQLException {
		StringBuffer sb = new StringBuffer();
		pStatement = connection.prepareStatement(SELECT_STUDENTS);		
		ResultSet resultSet = pStatement.executeQuery();
		sb.append(String.format("%6s ", "Id"));
		sb.append(String.format("%-20s ", "First Name"));
		sb.append(String.format("%-20s ", "Last Name"));
		sb.append(String.format("%5s ", "GPA"));
		sb.append(String.format("%6s   ", "SAT"));
		sb.append(String.format("%-15s\n", "Major"));
		
		sb.append(String.format("%6s ", "------"));
		sb.append(String.format("%-20s ", "--------------------"));
		sb.append(String.format("%-20s ", "--------------------"));
		sb.append(String.format("%5s ", "-----"));
		sb.append(String.format("%6s   ", "------"));
		sb.append(String.format("%-15s\n", "---------------"));
		while(resultSet.next()) {
			PreparedStatement pStatementMajor = 
					connection.prepareStatement
					("select name from major where id = ?");
			pStatementMajor.setInt(1, resultSet.getInt("major_id"));
			ResultSet majorResult = pStatementMajor.executeQuery();getClass();
			boolean hasAMajor = majorResult.next();
								
			sb.append(String.format("%6d ", resultSet.getInt("id")));
			sb.append(String.format("%-20s ", resultSet.getString("first_name")));
			sb.append(String.format("%-20s ", resultSet.getString("last_name")));
			sb.append(String.format("%5.1f ", resultSet.getFloat("gpa")));
			sb.append(String.format("%6d   ", resultSet.getInt("sat")));
			sb.append(String.format("%-15s\n", hasAMajor ? majorResult.getString("name") : "N/A"));
			
			pStatementMajor.close();
		}
		
		return sb.toString();
	}
	
	public void insertStudent(int id, String first, String last, double gpa, 
			int sat, String major) throws SQLException {
		PreparedStatement pStatement = connection.prepareStatement("select name from major where name = ?");
		pStatement.setString(1, major);
		ResultSet resultSet = pStatement.executeQuery();
		
		boolean hasAMajor = resultSet.next();
		
		pStatement = connection.prepareStatement("insert into student values (?, ?, ?, ?, ?, ?)");
		pStatement.setInt(1, id);
		pStatement.setString(2, first);
		pStatement.setString(3, last);
		pStatement.setDouble(4, gpa);
		pStatement.setInt(5, sat);
		if(hasAMajor) {
			pStatement.setInt(6, resultSet.getInt("id"));
		} else {
			pStatement.setNull(6, java.sql.Types.INTEGER);
		}
		
		pStatement.executeUpdate();
	}
	
	public void updateStudent(int id, double gpa, int sat, String major) throws SQLException {
		PreparedStatement pStatement = connection.prepareStatement("select id from major where name = ?");
		pStatement.setString(1, major);
		ResultSet resultSet = pStatement.executeQuery();
		
		boolean hasAMajor = resultSet.next();
		
		pStatement = connection.prepareStatement("update student set gpa = ?, sat = ?, major_id = ? where id = ?");
		pStatement.setDouble(1, gpa);
		pStatement.setInt(2, sat);
		if(hasAMajor) {
			pStatement.setInt(3, resultSet.getInt("id"));
		} else {
			pStatement.setNull(3, java.sql.Types.INTEGER);
		}
		pStatement.setInt(4, id);
		
		pStatement.executeUpdate();
	}
	
	public void deleteStudent(String last, int sat) throws SQLException {
		PreparedStatement pStatement = connection.prepareStatement("delete from student where last_name = ? and sat = ?");
		pStatement.setString(1, last);
		pStatement.setInt(2, sat);
		
		pStatement.executeUpdate();
	}
	
	public void backUp() throws SQLException {
		List<String> resultList = backUpInsert("Student");
		
		for(String result : resultList) {
			System.out.println(result);
		}
	}
	
	private List<String> backUpInsert(String tableName) throws SQLException {
		PreparedStatement pStatement = connection.prepareStatement("select * from " + tableName);
		ResultSet resultSet = pStatement.executeQuery();
		ResultSetMetaData rsmd = resultSet.getMetaData();
		
		List<String> insertStatements = new ArrayList<String>();
		
		while(resultSet.next()) {
			StringBuffer sbInsert = new StringBuffer();
			
			sbInsert.append("insert into " + tableName + " values (");
			switch(tableName) {
				case "Student":
					sbInsert.append(resultSet.getInt("id")).append(", '");
					sbInsert.append(resultSet.getString("first_name")).append("', '");
					sbInsert.append(resultSet.getString("last_name")).append("', ");
					sbInsert.append(resultSet.getDouble("gpa")).append(", ");
					sbInsert.append(resultSet.getInt("sat")).append(", ");
					sbInsert.append(resultSet.getInt("major_id"));
					break;
			}
			sbInsert.append(");");
			insertStatements.add(sbInsert.toString());
		}
		
		
		return insertStatements;
	}
}
