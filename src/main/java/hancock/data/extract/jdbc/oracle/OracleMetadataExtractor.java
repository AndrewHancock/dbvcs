package hancock.data.extract.jdbc.oracle;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import hancock.data.model.Field;
import hancock.data.model.FieldType;
import hancock.data.model.Table;

public class OracleMetadataExtractor {

	private static final String USER_TAB_COLS_QUERY = "SELECT\n" + "\tTABLE_NAME\n" + "\t,COLUMN_NAME\n"
			+ "\t,DATA_TYPE\n" + "FROM USER_TAB_COLS\n";
	private static final String CONSTRAINTS__QUERY = "select AC.TABLE_NAME, AC.CONSTRAINT_TYPE, AC.CONSTRAINT_NAME, AIC.COLUMN_NAME\n"
			+ "from all_constraints ac left outer join all_ind_columns aic\n "
			+ "on (ac.INDEX_NAME = aic.INDEX_NAME AND ac.TABLE_NAME = aic.TABLE_NAME)";

	private static Map<String, Set<String>> getConstraints(String jdbcString, Collection<Table> tables)
			throws SQLException {
		Map<String, Set<String>> tableUniqueFieldsMap = new HashMap<>();
		try (Connection con = DriverManager.getConnection(jdbcString); Statement stmt = con.createStatement()) {
			String tableList = tables.stream().map(t -> "UPPER('" + t.getTableName() + "')")
					.collect(Collectors.joining(","));
			String query = CONSTRAINTS__QUERY + "WHERE CONSTRAINT_TYPE IN ('P', 'U') AND ac.TABLE_NAME IN(" + tableList
					+ ") AND CONSTRAINT_TYPE IN ('P', 'U') order by AC.TABLE_NAME, ac.CONSTRAINT_TYPE, ac.CONSTRAINT_NAME, aic.COLUMN_POSITION ";
			ResultSet rs = stmt.executeQuery(query);

			String currentTableName = null;
			String currentConstraintName = null;

			while (rs.next()) {
				if (currentTableName == null || !currentTableName.equals(rs.getString(1))) {
					currentTableName = rs.getString(1);
					currentConstraintName = rs.getString(3);
					Set<String> unqiueFields = new HashSet<>();
					unqiueFields.add(rs.getString(4));
					tableUniqueFieldsMap.put(currentTableName, unqiueFields);
				}
				else if (currentConstraintName.equals(rs.getString(3))) {
					tableUniqueFieldsMap.get(currentTableName).add(rs.getString(4));
				}
			}
		}
		return tableUniqueFieldsMap;
	}

	private static FieldType mapFieldType(String oracleDataType) {
		switch (oracleDataType) {
		case "VARCHAR2":
		case "CHAR":
			return FieldType.STRING;
		case "DATE":
		case "TIMESTAMP":
			return FieldType.DATE;
		case "NUMBER":
			return FieldType.DECIMAL;
		default:
			throw new RuntimeException("Unrecognized oracle data type:" + oracleDataType);
		}
	}
	
	private static Map<String, Collection<Field>> getTableDefintions(String jdbcString, Collection<Table> tables)
			throws ClassNotFoundException, SQLException {
		Class.forName("oracle.jdbc.driver.OracleDriver");
		try (Connection con = DriverManager.getConnection(jdbcString); Statement stmt = con.createStatement()) {

			String tableList = tables.stream().map(t -> "UPPER('" + t.getTableName() + "')")
					.collect(Collectors.joining(","));
			String query = USER_TAB_COLS_QUERY + "WHERE UPPER(TABLE_NAME) IN (" + tableList
					+ ") ORDER BY TABLE_NAME, COLUMN_ID ";
			ResultSet rs = stmt.executeQuery(query);

			Map<String, Collection<Field>> tableInventory = new HashMap<>();
			String currentTableName = null;			

			while (rs.next()) {
				if (currentTableName == null ||!currentTableName.equals(rs.getString(1))) {
					currentTableName = rs.getString(1);					
					tableInventory.put(currentTableName, new ArrayList<Field>());
				}
				tableInventory.get(currentTableName).add(new Field(rs.getString(2), mapFieldType(rs.getString(3))));
			}
			
			return tableInventory;
		}
	}
	
	public static Collection<Table> extractMetadata(String jdbcString, Collection<Table> tables) throws ClassNotFoundException, SQLException {
			final Map<String, Collection<Field>> tableFieldMap = getTableDefintions(jdbcString, tables);
			final Map<String, Set<String>> tableUniqueFieldNameMap = getConstraints(jdbcString, tables); 
			
			return tables.stream()
				.map(table -> new Table(table.getTableName(), tableUniqueFieldNameMap.get(table.getTableName()), tableFieldMap.get(table.getTableName())))
				.collect(Collectors.toList());
		}
}

