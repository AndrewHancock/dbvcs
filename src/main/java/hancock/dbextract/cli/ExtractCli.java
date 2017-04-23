package hancock.dbextract.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class ExtractCli {
	public static ExtractOptions parseArguments(String args[], Options cliOptions) throws ParseException {
		ExtractOptions options = new ExtractOptions();
		CommandLineParser parser = new DefaultParser();
		CommandLine cmd = parser.parse(cliOptions, args);
		
		options.setJdbcPath(cmd.getOptionValue("j"));
		options.setOutputDirectory(cmd.getOptionValue("o"));
		options.setCataloguePath(cmd.getOptionValue("c"));
		return options; 
	}
	
	public  static Options getCliOptions() {
		Options options = new Options();
		options.addOption(OptionConstants.JDBC_URI, "jdbcUri", true, "JDBC URI locating the database to extract data from.");
		options.addOption(OptionConstants.OUTPUT_FOLDER, "outputFolder", true, "URI locating folder for extracted tables. Must not exists.");
		options.addOption(OptionConstants.CATALOGUE_URI, "dataCatalog", true, "URI locating data catalogue specifying the data to be extracted.");
		return options;
	}	
}
