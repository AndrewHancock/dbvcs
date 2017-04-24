package hancock.data.compare.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class CompareCli {
	public static Options getOptions() {
		Options options = new Options();
		options.addOption("l", "left", true, "Path of the left side repository to compare.");
		options.addOption("r", "right", true, "Path of the right side repository to compare.");
		options.addOption("o", "output", true, "Output path for comparison results. Directory must not exist.");
		
		return options;
	}
	
	public static CompareOptions parseOptions(String[] args, Options options) throws ParseException {
		CompareOptions opts = new CompareOptions();
		CommandLineParser parser = new DefaultParser();
		CommandLine cmd = parser.parse(options, args);
		
		if(cmd.hasOption('l')) {
			opts.setLeftPath(cmd.getOptionValue('l'));
		}
		else {
			throw new ParseException("Left path required.");
		}
		
		if(cmd.hasOption('r')) {
			opts.setRightPath(cmd.getOptionValue('r'));			
		}
		else {
			throw new ParseException("Right path required.");
		}
		
		if(cmd.hasOption('o' )) {
			opts.setOutputPath(cmd.getOptionValue('o'));
		}
		else {
			throw new ParseException("Output path required.");
		}
		return opts;
	}
	
}
