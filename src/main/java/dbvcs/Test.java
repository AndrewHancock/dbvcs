package dbvcs;


import java.io.IOException;
import java.lang.reflect.Type;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collection;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import dbvcs.metadata.model.Table;

public class Test {

	public static void main(String[] args) throws IOException, URISyntaxException {
		byte[] encoded = Files.readAllBytes(Paths.get(args[0]));
		  
		String json =   new String(encoded, Charset.defaultCharset());
		
		Gson gson = new GsonBuilder().setPrettyPrinting().create();		
		Type setType = new TypeToken<Collection<Table>>() {}.getType();
		Collection<Table>tables = gson.fromJson(json, setType);
		
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
