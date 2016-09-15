package ssa;

import java.sql.SQLException;

public class Mainline {

	public static void main(String[] args) throws SQLException {
		
		StudentDB studentDB = new StudentDB();
		
		studentDB.insertStudent("George", "Washington", 4.0, 1600, "");
		System.out.println(studentDB.printStudents());
		studentDB.updateStudent(3.5, 1450, "General Business");
		System.out.println(studentDB.printStudents());
		studentDB.deleteStudent("Washington", 1450);
		System.out.println(studentDB.printStudents());
				
		studentDB.backUp();
		
		DBConnect.close();
	}

}
