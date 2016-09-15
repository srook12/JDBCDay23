package ssa;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class StudentDB {
	private static Connection connection = null;
	private static PreparedStatement pStatement = null;
	
	private static final String SELECT_STUDENTS = "select * from student";
	
	// Table names
	public static final String INSTRUCTOR_TABLE = "instructor";
	public static final String STUDENT_TABLE = "student";
	
	public StudentDB() throws SQLException {
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
	
	public void insertStudent(String first, String last, double gpa, 
			int sat, String major) throws SQLException {
		PreparedStatement pStatement = connection.prepareStatement("select name from major where name = ?");
		pStatement.setString(1, major);
		ResultSet resultSet = pStatement.executeQuery();
		
		boolean hasAMajor = resultSet.next();
		
		pStatement = connection.prepareStatement("insert into student (first_name, last_name, gpa, sat, major_id) "
				+ "values (?, ?, ?, ?, ?)");
		pStatement.setString(1, first);
		pStatement.setString(2, last);
		pStatement.setDouble(3, gpa);
		pStatement.setInt(4, sat);
		if(hasAMajor) {
			pStatement.setInt(5, resultSet.getInt("id"));
		} else {
			pStatement.setNull(5, java.sql.Types.INTEGER);
		}
		pStatement.executeUpdate();
	}
	
	public void updateStudent(double gpa, int sat, String major) throws SQLException {
		PreparedStatement pStatement = connection.prepareStatement("select id from major where name = ?");
		pStatement.setString(1, major);
		ResultSet resultSet = pStatement.executeQuery();
		
		boolean hasAMajor = resultSet.next();
		
		pStatement = connection.prepareStatement("update student set gpa = ?, sat = ?, major_id = ? "
				+ "where first_name = ? and last_name = ?");
		pStatement.setDouble(1, gpa);
		pStatement.setInt(2, sat);
		if(hasAMajor) {
			pStatement.setInt(3, resultSet.getInt("id"));
		} else {
			pStatement.setNull(3, java.sql.Types.INTEGER);
		}
		pStatement.setString(4, "George");
		pStatement.setString(5, "Washington");
		
		pStatement.executeUpdate();
	}
	
	public void deleteStudent(String last, int sat) throws SQLException {
		PreparedStatement pStatement = connection.prepareStatement("delete from student where last_name = ? and sat = ?");
		pStatement.setString(1, last);
		pStatement.setInt(2, sat);
		
		pStatement.executeUpdate();
	}
	
	public void backUp() throws SQLException {		
		new BackUp(connection);		
	}
}
