package dbvcs.metadata.jdbc;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import dbvcs.metadata.model.Field;
import dbvcs.metadata.model.Table;

public class DatasetExtractor {

	private static final String QUERY = "SELECT\n" + "\tTABLE_NAME\n" + "\t,COLUMN_NAME\n" + "\t,DATA_TYPE\n"
			+ "FROM ALL_TAB_COLS\n";

	public static Collection<Table> extractMetadata(String jdbcString, Collection<Table> tables)
			throws ClassNotFoundException, SQLException {
		Class.forName("oracle.jdbc.driver.OracleDriver");
		try (Connection con = DriverManager.getConnection(jdbcString, "hr", "test");
				Statement stmt = con.createStatement()) {

			String tableList = tables.stream().map(t -> "UPPER('" + t.getTableName() + "')")
					.collect(Collectors.joining(","));
			String query = QUERY + "WHERE UPPER(TABLE_NAME) IN (" + tableList + ") ORDER BY TABLE_NAME, COLUMN_ID ";
			ResultSet rs = stmt.executeQuery(query);

			Collection<Table> tableInventory = new ArrayList<>();
			String currentTableName = null;
			Collection<Field> fields = new ArrayList<>();

			while (rs.next()) {
				if (currentTableName == null) {
					currentTableName = rs.getString(1);
				} else if (!currentTableName.equals(rs.getString(1))) {
					tableInventory.add(new Table(currentTableName, fields));
					currentTableName = rs.getString(1);
					fields = new ArrayList<>();
				}
				fields.add(new Field(rs.getString(2)));

			}
			tableInventory.add(new Table(currentTableName, fields));
			return tableInventory;
		}
	}

	public static void extractTables(String jdbcString, Collection<Table> tables, String targetFolder) throws SQLException, IOException {
		for (Table table : tables) {
			try (Connection con = DriverManager.getConnection(jdbcString, "hr", "test");
					Statement stmt = con.createStatement();
					CSVPrinter printer = new CSVPrinter( new FileWriter(Paths.get(targetFolder, table.getTableName() + ".csv").toString()), CSVFormat.RFC4180)) {
				String queryStr = "SELECT\n\t" +  table.getFields().stream().map(f -> f.getName()).collect(Collectors.joining("\n\t,")) + "\nFROM " + table.getTableName();
				ResultSet rs = stmt.executeQuery(queryStr);
				
				printer.printRecords(rs);
			}

		}
	}
}
