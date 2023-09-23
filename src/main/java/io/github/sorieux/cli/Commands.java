package io.github.sorieux.cli;

import org.apache.calcite.piglet.PigConverter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Programs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.calcite.piglet.PigConverter.create;


@CommandLine.Command(
        description = "ConvertPigToSQL - A tool to convert Pig scripts to SQL.",
        mixinStandardHelpOptions = true, // Adds standard help options like --help
        version = "1.0"
)
public class Commands implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(Commands.class);

    // Configuration for the Calcite framework
    private final FrameworkConfig config = Frameworks.newConfigBuilder()
            .parserConfig(SqlParser.Config.DEFAULT)
            .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2))
            .build();

    // Setting the SQL dialect to ANSI
    private final SqlDialect dialect = AnsiSqlDialect.DEFAULT;

    @Option(names = {"-s", "--string"}, description = "String containing the Pig script to convert.")
    private String pigString;

    @Option(names = {"-i", "--inputFile"}, description = "Path to the Pig file to convert.")
    private String inputFilePath;

    @Option(names = {"-o", "--outputFile"}, description = "Path to the output SQL file.")
    private String outputFilePath;

    @Option(names = {"-id", "--inputDirectory"}, description = "Directory containing Pig files to convert.")
    private String inputDirectoryPath;

    @Option(names = {"-od", "--outputDirectory"}, description = "Directory to store the converted SQL files.")
    private String outputDirectoryPath;

    public static void main(String[] args) {
        int exitCode = new CommandLine(new Commands()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public void run() {
        // Determine mode based on provided options and perform conversion
        if (pigString != null) {
            String sql = convertPigStringToSQL(pigString);
            System.out.println(sql);
        } else if (inputFilePath != null && outputFilePath != null) {
            convertPigFileToSQL(inputFilePath, outputFilePath);
            System.out.printf("File converted and saved to: %s%n", outputFilePath);
        } else if (inputDirectoryPath != null && outputDirectoryPath != null) {
            Map<String, Boolean> conversionResults = convertPigDirectoryToSQL(inputDirectoryPath, outputDirectoryPath);
            conversionResults.forEach((file, success) -> {
                String status = success ? "[SUCCESS]" : "[FAILURE]";
                System.out.printf("%s Conversion of %s%n", status, file);
            });
        } else {
            throw new CommandLine.ParameterException(
                    new CommandLine(this),
                    "Incorrect parameters. Use --help for available options."
            );
        }
    }

    /**
     * Converts a given Pig script string to SQL.
     *
     * @param pig The Pig script string.
     * @return The converted SQL string.
     */
    protected String convertPigStringToSQL(String pig) {
        String sql;
        logger.debug("Pig String : {}", pig);

        try {
            // Convert Pig script to Relational Algebra
            PigConverter converter2Rel = create(config);
            RelNode rel = converter2Rel.pigQuery2Rel(pig).get(0);

            // Convert Relational Algebra to SQL
            RelToSqlConverter converter2Sql = new RelToSqlConverter(dialect);
            SqlNode sqlNode = converter2Sql.visitRoot(rel).asStatement();

            SqlPrettyWriter writer = new SqlPrettyWriter(dialect);
            sql = writer.format(sqlNode);
        } catch (Exception e) {
            throw new RuntimeException("Error converting Pig string to SQL.", e);
        }

        logger.debug("Sql result : {}", sql);

        return sql;
    }

    /**
     * Converts a given Pig file to SQL and saves it to a specified output file.
     *
     * @param inputFile  Path to the Pig file to be converted.
     * @param outputFile Path where the converted SQL should be saved.
     */
    protected void convertPigFileToSQL(String inputFile, String outputFile) {
        try {
            logger.debug("inputFile : {}", inputFile);
            logger.debug("outputFile : {}", outputFile);


            Map<String, String> params = new HashMap<>();
            params.put("input", inputFile);
            params.put("output", "outputFile");

            // Convert Pig script to Relational Algebra
            PigConverter converter2Rel = create(config);
            RelNode rel = converter2Rel.pigScript2Rel(inputFile, params, true).get(0);

            // Convert Relational Algebra to SQL
            RelToSqlConverter converter2Sql = new RelToSqlConverter(dialect);
            SqlNode sqlNode = converter2Sql.visitRoot(rel).asStatement();

            SqlPrettyWriter sqlWriter = new SqlPrettyWriter(dialect);
            String sql = sqlWriter.format(sqlNode);

            logger.debug("Sql result : {}", sql);

            // Save the converted SQL to the output file
            try (BufferedWriter outputWriter = new BufferedWriter(new FileWriter(outputFile))) {
                outputWriter.write(sql);
            }
        } catch (Exception e) {
            throw new RuntimeException("Error converting Pig file to SQL.", e);
        }
    }

    /**
     * Converts all Pig files in a given directory to SQL and saves them in a specified output directory.
     *
     * @param inputDir  Directory containing Pig files to convert.
     * @param outputDir Directory to store the converted SQL files.
     * @return A map containing the names of the processed files and a boolean indicating the success of the conversion.
     */
    protected Map<String, Boolean> convertPigDirectoryToSQL(String inputDir, String outputDir) {
        logger.debug("inputFile : {}", inputDir);
        logger.debug("outputFile : {}", outputDir);

        File inputDirectory = new File(inputDir);
        File outputDirectory = new File(outputDir);

        if (!inputDirectory.exists() || !inputDirectory.isDirectory()) {
            throw new RuntimeException("Input directory doesn't exist or is not a directory: " + inputDir);
        }

        if (!outputDirectory.exists()) {
            outputDirectory.mkdir();
        }

        File[] pigFiles = inputDirectory.listFiles((dir, name) -> name.endsWith(".pig"));
        Map<String, Boolean> conversionResults = new HashMap<>();

        for (File pigFile : pigFiles) {
            String outputFileName = pigFile.getName().replace(".pig", ".sql");
            File outputFile = new File(outputDirectory, outputFileName);

            try {
                convertPigFileToSQL(pigFile.getAbsolutePath(), outputFile.getAbsolutePath());
                conversionResults.put(pigFile.getName(), true);
            } catch (Exception e) {
                conversionResults.put(pigFile.getName(), false);
            }
        }

        return conversionResults;
    }
}
