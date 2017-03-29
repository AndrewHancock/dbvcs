package dbvcs;


import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import dbvcs.metadata.model.Table;

public class Test {

	public static void main(String[] args) {
		Set<Table> tableSet = new HashSet<>();
		tableSet.add(new Table("testTable1", new HashSet<>(Arrays.asList(new String[] { "field1", "field2", "field3"} ))));
		tableSet.add(new Table("testTable2", new HashSet<>(Arrays.asList(new String[] { "field_abc", "field_def", "field_hij"} ))));
		
		
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		
		String json = gson.toJson(tableSet);
		
		System.out.println(gson.toJson(tableSet));
		
		Type setType = new TypeToken<Set<Table>>() {}.getType();
		tableSet = gson.fromJson(json, setType);
		
		testSql();

	}

	private static void testSql() 
	{
		try {
			// step1 load the driver class
			Class.forName("oracle.jdbc.driver.OracleDriver");

			// step2 create the connection object
			Connection con = DriverManager.getConnection("jdbc:oracle:thin:@192.168.1.232:50001:xe", "hr", "test");

			// step3 create the statement object
			Statement stmt = con.createStatement();

			// step4 execute query
			ResultSet rs = stmt.executeQuery("select * from employees");
			while (rs.next())
				System.out.println(rs.getInt(1) + "  " + rs.getString(2) + "  " + rs.getString(3));

			// step5 close the connection object
			con.close();

		} catch (Exception e) {
			System.out.println(e);
		}
	}
}
