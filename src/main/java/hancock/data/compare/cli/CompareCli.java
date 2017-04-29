package hancock.data.compare.cli;

import java.util.function.Consumer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class CompareCli {
	public static Options getOptions() {
		Options options = new Options();
		options.addOption("l", "left", true, "Path of the left side repository to compare.");
		options.addOption("r", "right", true, "Path of the right side repository to compare.");
		options.addOption("o", "output", true, "Output path for comparison results. Directory must not exist.");
		options.addOption("f", "format", true, "File format: csv or partquet");
		options.addOption("c", "compression", true, "Compression code: gzip or snappy");
		
		return options;
	}
	
	private static void applyOption(Option option, CommandLine cmd, Consumer<String> consumer ) throws ParseException {
			if(cmd.hasOption(option.getOpt())) {
				consumer.accept(cmd.getOptionValue(option.getOpt()));	
			} 
			else {
				throw new ParseException(option.getLongOpt() + " required.");
			}
		
	}
	
	public static CompareOptions parseOptions(String[] args, Options options) throws ParseException {
		CompareOptions opts = new CompareOptions();
		CommandLineParser parser = new DefaultParser();
		CommandLine cmd = parser.parse(options, args);
	
		applyOption(options.getOption("l"), cmd, opts::setLeftPath);
		applyOption(options.getOption("r"), cmd, opts::setRightPath);
		applyOption(options.getOption("o"), cmd, opts::setOutputPath);
		applyOption(options.getOption("f"), cmd, opts::setFormat);
		applyOption(options.getOption("c"), cmd, opts::setCompressionCodec);
		
		return opts;
	}
	
	
	
	
	
}
