package hancock.data.extract.cli;

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
		
		options.setJdbcPath(cmd.getOptionValue(OptionConstants.JDBC_URI));
		options.setOutputDirectory(cmd.getOptionValue(OptionConstants.OUTPUT_FOLDER));
		options.setCataloguePath(cmd.getOptionValue(OptionConstants.EXTRACT_CONFIG_PATH));		
		options.setFormat(cmd.getOptionValue(OptionConstants.FORMAT));
		options.setCompressionCodec(cmd.getOptionValue(OptionConstants.COMPRESSION_CODEC));
		return options; 
	}
	
	public  static Options getCliOptions() {
		Options options = new Options();
		options.addOption(OptionConstants.JDBC_URI, "jdbc-uri", true, "JDBC URI locating the database to extract data from.");
		options.addOption(OptionConstants.OUTPUT_FOLDER, "output-folder", true, "URI locating folder for extracted tables. Must not exists.");
		options.addOption(OptionConstants.EXTRACT_CONFIG_PATH, "extract-config", true, "Path to the configuration file for this extract.");
		options.addOption(OptionConstants.FORMAT, "format", true, "File format: CSV or parquet");
		options.addOption(OptionConstants.COMPRESSION_CODEC, "compression-codec", true, "Compression code: gzip or snappy");
		return options;
	}	
}
