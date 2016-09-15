package ssa;

import java.sql.Connection;
import java.sql.Statement;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


public class BackUp {
	private static Connection conn = null;
	private static DatabaseMetaData dmd = null;
	
	private static MetaDataContent tableNames = new MetaDataContent();
	
	private static ResultSet cols = null;
	private static MetaDataContent colNames = new MetaDataContent();
	private static MetaDataContent colTypes = new MetaDataContent();
	private static MetaDataContent colSize = new MetaDataContent();
	
	private static ArrayList<String> dependencyOrder = new ArrayList<String>();
	
	public static final String DATABASE_NAME = "tiy";
	
	public BackUp(Connection conn) {
		BackUp.conn = conn;
		
		try {
			dmd = BackUp.conn.getMetaData();
			StringBuffer backupFile = new StringBuffer();
			
			// Use database
			backupFile.append("use " + DATABASE_NAME).append("\n\n");
			
			// Determine the dependency order for table deletions, which can
			// be reversed for table creations			
			generateDependencyOrder();
			
			// Generate the delete statements using the dependency order
			backupFile.append(backupDeletes()).append("\n");
			
			backupFile.append(backupCreates()).append("\n");
			
			backupFile.append(backupInserts());
			
			System.out.println(backupFile.toString());
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}
	
	private void generateDependencyOrder() throws SQLException {
		extractTableNames();
		
		int idx = 0;
		
		// Cycle through the table names to find the next table
		// where the dependencies have been satisfied
		while(!tableNames.isEmpty()) {
			idx %= tableNames.size();
			
			if(checkForeignKeys(tableNames.get(idx))) {
				dependencyOrder.add(tableNames.remove(idx));
			} else {		
				idx++;
			}
		}
	}
		
	private boolean checkForeignKeys(String tableName) throws SQLException {
		boolean result = true;
		
		ResultSet foreignKeys = dmd.getExportedKeys(conn.getCatalog(), null, tableName);
	    while (foreignKeys.next()) {
	        String fkTableName = foreignKeys.getString("fktable_name");

	        // Covers the situation where a table could have a self-referencing foreign key
	        if(!dependencyOrder.contains(fkTableName) && !tableName.equals(fkTableName)) {
	        	result = false;
	        	break;
	        }
	    }
		
		return result;
	}
	
	private String backupDeletes() throws SQLException {
		StringBuffer sbDelete = new StringBuffer();
		
		if (dependencyOrder.size() > 0) {
			sbDelete.append("-- Drop the tables if they exist").append("\n");
		}
		for(String tableName : dependencyOrder) {
			sbDelete.append("drop table if exists " + tableName + ";").append("\n");
		}
		
		return sbDelete.toString();
	}
		
	private void extractTableNames() throws SQLException {		
		ResultSet rsTables = dmd.getTables(null, null, null, null);
		
		while(rsTables.next()) {			
			tableNames.add(rsTables.getString("table_name"));
		}
	}
	
	private void extractColData(String tableName) throws SQLException {
		colNames.clear();
		colTypes.clear();
		colSize.clear();
		
		cols = dmd.getColumns(null, null, tableName, null);		
		
		while(cols.next()) {
			colNames.add(cols.getString("column_name"));
			colTypes.add(cols.getString("type_name"));
			colSize.add(cols.getString("column_size"));
		}
	}
	
	private String backupCreates() throws SQLException {
		StringBuffer sbCreate = new StringBuffer();
				
		Collections.reverse(dependencyOrder);
		
		for(String tableName : dependencyOrder) {
			extractColData(tableName);
			
			String sql = "select * from " + tableName;
			Statement statement = conn.createStatement();
			ResultSet resultSet = statement.executeQuery(sql);
			ResultSetMetaData rsmd = resultSet.getMetaData();
			
			sbCreate.append("create table " + tableName + "(");
			for(int idx = 1; idx <= rsmd.getColumnCount(); idx++) {
				sbCreate.append(rsmd.getColumnName(idx)).append(" ").append(rsmd.getColumnTypeName(idx));
				
				if(rsmd.getColumnTypeName(idx).equalsIgnoreCase("DECIMAL")) {
					sbCreate.append("(" + rsmd.getPrecision(idx) + ", " + rsmd.getScale(idx) + ")");
				} else {				
					sbCreate.append("(" + rsmd.getColumnDisplaySize(idx) + ")");
				}		
				
				// auto-increment?
				if(rsmd.isAutoIncrement(idx)) {
					sbCreate.append(" auto_increment");
				}
				
				// null?
				if(rsmd.isNullable(idx) == ResultSetMetaData.columnNoNulls) {
					sbCreate.append(" not null");
				}
				
				/*
				ResultSet primaryKeys = dmd.getPrimaryKeys(null, null, tableName);
				primaryKeys.getMetaData().is
				if(col)
				*/
				if(idx <= colNames.size() - 1) {
					sbCreate.append(", ");
				}
			}
			
			sbCreate.append(writePrimaryKeys(tableName));
			sbCreate.append(writeForeignKeys(tableName));
			
			sbCreate.append(");\n");
		}
		
		
		return sbCreate.toString();
	}
	
	private String writePrimaryKeys(String tableName) throws SQLException {
		StringBuffer sbPKs = new StringBuffer();
		
		ResultSet primaryKeys = dmd.getPrimaryKeys(null, null, tableName);
		
		if(primaryKeys.next()) {
			sbPKs.append(", primary key(");
			
			do {
				sbPKs.append(primaryKeys.getString("column_name"));
			
				if(!primaryKeys.isLast()) {
					sbPKs.append(", ");
				}
			} while(primaryKeys.next());
			
			sbPKs.append(")");
		}
		
		return sbPKs.toString();
	}
	
	private String writeForeignKeys(String tableName) throws SQLException {
		StringBuffer sbFKs = new StringBuffer();
		
		ResultSet foreignKeys = dmd.getImportedKeys(conn.getCatalog(), null, tableName);
				
	    while (foreignKeys.next()) {
	        String fkTableName = foreignKeys.getString("fktable_name");
	        String fkColumnName = foreignKeys.getString("fkcolumn_name");
	        String pkTableName = foreignKeys.getString("pktable_name");
	        String pkColumnName = foreignKeys.getString("pkcolumn_name");
	                		
	        sbFKs.append(", foreign key (").append(fkColumnName).append(") ");
	        sbFKs.append("references " + pkTableName + "(" + pkColumnName + ")");
	    }
		
		return sbFKs.toString();
	}
	
	private String backupInserts() throws SQLException {
		extractTableNames();		
		Map<String, MetaDataContent> insertionsMap = new HashMap<String, MetaDataContent>();
		
		StringBuffer sbInsert = new StringBuffer();
		
		for(String tableName : tableNames) {
			extractColData(tableName);
			insertionsMap.put(tableName, generateTableInserts(tableName));
		}
							
		for(String key : dependencyOrder) {
			sbInsert.append("-- Insertions into " + key).append("\n");
			for(String result : insertionsMap.get(key)) {
				sbInsert.append(result).append("\n");
			}
			sbInsert.append("\n");
		}
		
		return sbInsert.toString();
	}
	
	private MetaDataContent generateTableInserts(String tableName) throws SQLException {
		PreparedStatement pStatement = conn.prepareStatement("select * from " + tableName);
		ResultSet resultSet = pStatement.executeQuery();
		
		MetaDataContent insertStatements = new MetaDataContent();
				
		while(resultSet.next()) {
			StringBuffer sbInsert = new StringBuffer();
			
			sbInsert.append("insert into " + tableName + " (");
			
			for(int idx = 0; idx < colNames.size(); idx++) {
				sbInsert.append(colNames.get(idx));
				if(idx < colNames.size() - 1) {
					sbInsert.append(", ");
				}
			}
			
			sbInsert.append(") values(");
			
			for(int idx = 0; idx < colTypes.size(); idx++) {
				String stringResult = null;
				
				if(colTypes.get(idx).equalsIgnoreCase("VARCHAR")) {
					stringResult = resultSet.getString(idx+1);
					
					if(resultSet.wasNull()) {
						stringResult = "NULL";
						sbInsert.append(stringResult);
					} else {
						sbInsert.append("'").append(stringResult).append("'");
					}					
					
				} else {
					if (colTypes.get(idx).equalsIgnoreCase("INT") ||
					       colTypes.get(idx).equalsIgnoreCase("TINYINT")) {
						stringResult = String.valueOf(resultSet.getInt(idx+1));
					} else if (colTypes.get(idx).equalsIgnoreCase("DECIMAL")) {
						stringResult = String.valueOf(resultSet.getDouble(idx+1));					
					}
					
					if(resultSet.wasNull()) {
						stringResult = "NULL";
					}
					
					sbInsert.append(stringResult);
				}
				
				if(idx < colTypes.size() - 1) {
					sbInsert.append(", ");
				}
			}
			
			sbInsert.append(");");
			insertStatements.add(sbInsert.toString());
		}
		
		
		return insertStatements;
	}
}
