package hancock.data.extract.jdbc.oracle;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Collectors;

import hancock.data.model.Field;
import hancock.data.model.Table;

public class OracleMetadataExtractor {

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
}
