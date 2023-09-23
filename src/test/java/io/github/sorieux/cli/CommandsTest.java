package io.github.sorieux.cli;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.io.File;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CommandsTest {

    private Commands commands;

    private static final String BASE_DIR = "src/test/resources/";

    String expectedSQLTestPig = "SELECT `query`, CAST(SUM(`count`) AS BIGINT) AS `count`\n" +
            "FROM `testPig`.`pig`\n" +
            "GROUP BY `query`\n" +
            "ORDER BY 2 DESC\n" +
            "FETCH NEXT 5 ROWS ONLY";

    @BeforeEach
    void setUp() {
        commands = new Commands();
    }

    @Test
    void testConvertPigStringToSQL() {
        String pigScript = "data = LOAD 'input.txt' USING PigStorage(',') AS (id:int, name:chararray, age:int);\n" +
                "filtered_data = FILTER data BY age > 25;\n" +
                "grouped_data = GROUP filtered_data BY name;\n" +
                "count_data = FOREACH grouped_data GENERATE group AS name, COUNT(filtered_data) AS count;\n" +
                "STORE count_data INTO 'output.txt' USING PigStorage(',');\n";
        String expectedSQL = "SELECT `name`, CAST(COUNT(*) AS BIGINT) AS `count`\n" +
                "FROM `input`.`txt`\n" +
                "WHERE `age` > 25\n" +
                "GROUP BY `name`";

        String result = commands.convertPigStringToSQL(pigScript);
        assertEquals(expectedSQL, result);
    }

    @Test
    void testConvertPigFileToSQL() throws IOException {
        String inputFilePath = Paths.get(BASE_DIR, "testPig.pig").toString();
        String outputFilePath = Paths.get(BASE_DIR, "testPig.sql").toString();

        commands.convertPigFileToSQL(inputFilePath, outputFilePath);
        String result = new String(Files.readAllBytes(Paths.get(outputFilePath)), StandardCharsets.UTF_8);
        assertEquals(expectedSQLTestPig, result);

        // Cleanup after this test
        deleteIfExists(Paths.get(outputFilePath));
    }

    @Test
    void testConvertPigDirectoryToSQL() throws IOException {
        String inputDirectoryPath = BASE_DIR;
        String outputDirectoryPath = Paths.get(BASE_DIR, "/tmp").toString();

        Map<String, Boolean> results = commands.convertPigDirectoryToSQL(inputDirectoryPath, outputDirectoryPath);
        for (Map.Entry<String, Boolean> entry : results.entrySet()) {
            assertTrue(entry.getValue(), "Conversion failed for file: " + entry.getKey());
        }

        // Cleanup after this test
        deleteDirectory(Paths.get(outputDirectoryPath));
    }

    @Test
    void testInvalidParameters() {
        //TODO
    }

    @Test
    void testConversionError() {
        String invalidPigScript = "INVALID_PIG_SCRIPT";
        assertThrows(RuntimeException.class, () -> commands.convertPigStringToSQL(invalidPigScript));
    }

    @Test
    void testEmptyInputDirectory() throws IOException {
        String emptyDir = Paths.get(BASE_DIR, "emptyDirectory").toString();
        Files.createDirectories(Paths.get(emptyDir));

        String outputDirectoryPath = Paths.get(BASE_DIR, "/tmpEmpty").toString();

        Map<String, Boolean> results = commands.convertPigDirectoryToSQL(emptyDir, outputDirectoryPath);
        assertTrue(results.isEmpty(), "No files should be processed from an empty directory");

        // Cleanup after this test
        deleteDirectory(Paths.get(outputDirectoryPath));
        deleteDirectory(Paths.get(emptyDir));
    }

    private void deleteIfExists(Path path) throws IOException {
        if (Files.exists(path)) {
            Files.delete(path);
        }
    }

    private void deleteDirectory(Path dirPath) throws IOException {
        if (Files.exists(dirPath) && Files.isDirectory(dirPath)) {
            Files.walk(dirPath)
                    .sorted(java.util.Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }
    }
}
