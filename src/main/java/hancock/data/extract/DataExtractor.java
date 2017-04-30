package hancock.data.extract;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.spark.sql.SparkSession;

import hancock.data.extract.cli.ExtractCli;
import hancock.data.extract.cli.ExtractOptions;
import hancock.data.extract.jdbc.oracle.OracleMetadataExtractor;
import hancock.data.model.Commit;
import hancock.data.model.Table;
import hancock.data.model.serialization.JsonTableSerializer;

public class DataExtractor {

	private static void createTargetDirectory(String targetDir) throws Exception {
		if (Files.isDirectory(Paths.get(targetDir))) {
			throw new Exception("Target directory already exists.");
		} else {
			Files.createDirectory(Paths.get(targetDir));
		}
	}

	public static void main(String[] args) throws Exception {

		Options cliOptions = ExtractCli.getCliOptions();
		ExtractOptions options = null;
		try {

			options = ExtractCli.parseArguments(args, cliOptions);
		} catch (ParseException e) {
			new HelpFormatter().printHelp("DataExtractor", cliOptions);
			System.exit(-1);
		}

		createTargetDirectory(options.getOutputDirectory());
		Commit extractConfig = JsonTableSerializer.readCommitJson(Paths.get(options.getCataloguePath()));
		Commit manifest = new Commit(options.getFormat(), options.getCompressionCodec(), OracleMetadataExtractor.extractMetadata(options.getJdbcPath(), extractConfig.getTables()));
		JsonTableSerializer.writeCommitJson(Paths.get(options.getOutputDirectory(), "metadata.json"), manifest);

		SparkSession spark = SparkSession.builder().master("local").appName("SparkCSVExample").getOrCreate();

		TableExtractor extractor = new TableExtractor(spark, options.getJdbcPath(), options.getFormat(), options.getCompressionCodec());

		final String outputDirectory = options.getOutputDirectory();
		manifest.getTables().forEach(table -> extractor.extractTable(table, Paths.get(outputDirectory, table.getTableName())));
	}

}
