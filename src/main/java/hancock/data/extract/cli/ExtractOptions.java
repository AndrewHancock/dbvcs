package hancock.data.extract.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class ExtractOptions {
	private String jdbcPath;
	private String outputDirectory;
	private String cataloguePath;
	private String userName;
	private String password;
	
	public String getJdbcPath() {
		return jdbcPath;
	}
	
	public void setJdbcPath(String jdbcPath) {
		this.jdbcPath = jdbcPath;
	}
	
	public String getOutputDirectory() {
		return outputDirectory;
	}
	
	public void setOutputDirectory(String outputDirectory) {
		this.outputDirectory = outputDirectory;
	}
	
	public String getCataloguePath() {
		return cataloguePath;
	}
	
	public void setCataloguePath(String cataloguePath) {
		this.cataloguePath = cataloguePath;
	}
	
	public String getUserName() {
		return userName;
	}
	
	public void setUserName(String userName) {
		this.userName = userName;
	}
	
	public String getPassword() {
		return password;
	}
	
	public void setPassword(String password) {
		this.password = password;
	}

}
