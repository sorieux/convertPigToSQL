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


    String expectedSQLTestPig2 = "SELECT `date`, CAST(SUM(`amount`) AS DOUBLE) AS `totalSales`\n" +
            "FROM `transactions`.`csv`\n" +
            "WHERE `category` = 'Electronique'\n" +
            "GROUP BY `date`\n" +
            "\n" +
            "SELECT `csv1`.`city`, CAST(SUM(`csv0`.`amount`) AS DOUBLE) AS `totalCitySales`\n" +
            "FROM `transactions`.`csv` AS `csv0`\n" +
            "    INNER JOIN `customers`.`csv` AS `csv1` ON `csv0`.`id` = `csv1`.`id`\n" +
            "WHERE `csv0`.`amount` > 100\n" +
            "GROUP BY `csv1`.`city`\n" +
            "ORDER BY 2 DESC";

    String expectedSQLTestPigUdf = "toto";

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
        assertEquals(expectedSQL, result.trim());
    }

    @Test
    void testConvertPigFileToSQL() throws IOException {
        String inputFilePath = Paths.get(BASE_DIR, "testPig.pig").toString();
        String outputFilePath = Paths.get(BASE_DIR, "testPig.sql").toString();

        commands.convertPigFileToSQL(inputFilePath, outputFilePath);
        String result = new String(Files.readAllBytes(Paths.get(outputFilePath)), StandardCharsets.UTF_8);
        assertEquals(expectedSQLTestPig, result.trim());

        // Cleanup after this test
        deleteIfExists(Paths.get(outputFilePath));
    }

    @Test
    void testConvertPigFileToSQL2() throws IOException {
        String inputFilePath = Paths.get(BASE_DIR, "pig_script_join_multi_store.pig").toString();
        String outputFilePath = Paths.get(BASE_DIR, "pig_script_join_multi_store.sql").toString();

        commands.convertPigFileToSQL(inputFilePath, outputFilePath);
        String result = new String(Files.readAllBytes(Paths.get(outputFilePath)), StandardCharsets.UTF_8);
        assertEquals(expectedSQLTestPig2, result.trim());

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


    @Test
    void testConvertPigStringToSQLWithMacro() {
        String pigScript = "DEFINE myfilter(relvar,colvar) returns x{\n" +
                "$x = filter $relvar by $colvar==15;\n" +
                "};" +
                "emp = load '/data/employee'using PigStorage(',') as (eno,ename,sal,dno);\n" +
                "empdno15 =myfilter( emp,dno);\n";
        String expectedSQL = "SELECT *\n" +
                "FROM `/data/employee`\n" +
                "WHERE CAST(`dno` AS INTEGER) = 15";

        String result = commands.convertPigStringToSQL(pigScript);
        assertEquals(expectedSQL, result.trim());
    }

    @Test
    void testConvertPigStringToSQLWithExternalMacro() {
        String pathToMacro = Paths.get("src", "test", "resources", "myfilter.macro").toAbsolutePath().toString();

        String pigScript = String.format("IMPORT '%s';\n" +
                "emp = load '/data/employee'using PigStorage(',') as (eno,ename,sal,dno);\n" +
                "empdno15 =myfilter( emp,dno);\n", pathToMacro);
        String expectedSQL = "SELECT *\n" +
                "FROM `/data/employee`\n" +
                "WHERE CAST(`dno` AS INTEGER) = 15";

        String result = commands.convertPigStringToSQL(pigScript);
        assertEquals(expectedSQL, result.trim());
    }

    @Test
    void testConvertPigStringToSQLWithCONCATFunction() {
        String pigScript = String.format("data = LOAD 'input.txt' USING PigStorage(',') AS (first_name:chararray, last_name:chararray, age:int);\n" +
                "full_name_data = FOREACH data GENERATE CONCAT(first_name, last_name) AS full_name, age;\n" +
                "STORE full_name_data INTO 'output.txt' USING PigStorage(',');\n");
        String expectedSQL = "SELECT `first_name` || `last_name` AS `full_name`, `age`\n" +
                "FROM `input`.`txt`";

        String result = commands.convertPigStringToSQL(pigScript);
        assertEquals(expectedSQL, result.trim());
    }

    @Test
    void testConvertComplexPigString() {
        String pigScript = String.format("-- Load the data\n" +
                "sales = LOAD 'sales.txt' USING PigStorage(',') AS (item_id:chararray, amount:int);\n" +
                "items = LOAD 'items.txt' USING PigStorage(',') AS (item_id:chararray, item_name:chararray, category:chararray, price:int);\n" +
                "\n" +
                "-- Join sales with items on item_id\n" +
                "joined_data = JOIN sales BY item_id, items BY item_id;\n" +
                "\n" +
                "-- Group by category\n" +
                "grouped_data = GROUP joined_data BY category;\n" +
                "\n" +
                "-- Calculate total sales for each category\n" +
                "sum_data = FOREACH grouped_data GENERATE group AS category, SUM(joined_data.amount) AS total_sales;\n" +
                "\n" +
                "-- Filter the results to only include categories with total_sales > 20\n" +
                "filtered_data = FILTER sum_data BY total_sales > 20;\n" +
                "\n" +
                "-- Sort the data by total_sales in descending order\n" +
                "sorted_data = ORDER filtered_data BY total_sales DESC;\n" +
                "\n");
        String expectedSQL = "SELECT `txt0`.`category`, CAST(SUM(`txt`.`amount`) AS BIGINT) AS `total_sales`\n" +
                "FROM `sales`.`txt`\n" +
                "    INNER JOIN `items`.`txt` AS `txt0` ON `txt`.`item_id` = `txt0`.`item_id`\n" +
                "GROUP BY `txt0`.`category`\n" +
                "HAVING CAST(SUM(`txt`.`amount`) AS BIGINT) > 20\n" +
                "ORDER BY 2 DESC";

        String result = commands.convertPigStringToSQL(pigScript);
        assertEquals(expectedSQL, result.trim());
    }

    @Test
    void testConvertPigStringForeach() {
        String pigScript = String.format("students_data = LOAD 'students.txt' USING PigStorage(',') \n" +
                "    AS (name:chararray, score:int, year_of_birth:int);\n" +
                "\n" +
                "students_with_status = FOREACH students_data GENERATE \n" +
                "    name AS name:chararray, \n" +
                "    score AS score:int, \n" +
                "    year_of_birth AS year_of_birth:int,\n" +
                "    2023 - year_of_birth AS age:int,\n" +
                "    (score > 50 ? 'Passed' : 'Failed') AS status:chararray;\n");
        String expectedSQL = "SELECT `name`, `score`, `year_of_birth`, 2023 - `year_of_birth` AS `age`, CASE WHEN `score` > 50 THEN 'Passed' ELSE 'Failed' END AS `status`\n" +
                "FROM `students`.`txt`";

        String result = commands.convertPigStringToSQL(pigScript);
        assertEquals(expectedSQL, result.trim());
    }

    @Test
    void testConvertPigStringToSQLWithUDF() {
        String pathToUdf = Paths.get("libs", "piggybank-0.16.0.jar").toAbsolutePath().toString();

        String pigScript = String.format("REGISTER 'libs/piggybank-0.16.0.jar';\n" +
                "DEFINE Upper org.apache.pig.piggybank.evaluation.string.UPPER();\n" +
                "data = LOAD 'data.txt' USING PigStorage(',') AS (name:chararray, age:int);\n" +
                "uppercased_data = FOREACH data GENERATE Upper(name) AS upper_name, age;\n" +
                "STORE uppercased_data INTO 'output.txt' USING PigStorage(',');\n", pathToUdf);
        String expectedSQL = "SELECT UPPER(`PIG_TUPLE`(`name`)) AS `upper_name`, `age`\n" +
                "FROM `data`.`txt`";

        String result = commands.convertPigStringToSQL(pigScript);
        assertEquals(expectedSQL, result.trim());
    }

    @Test
    void testConvertPigStringToSQLWithSubstringUDF() {
        String pathToUdf = Paths.get("libs", "piggybank-0.16.0.jar").toAbsolutePath().toString();

        String pigScript = String.format("REGISTER '%s';\n" +
                "data = LOAD 'data.txt' USING PigStorage(',') AS (name:chararray, age:int);\n" +
                "sub_string_data = FOREACH data GENERATE org.apache.pig.piggybank.evaluation.string.SUBSTRING(name, 0, 3) AS short_name, age;\n" +
                "STORE sub_string_data INTO 'output.txt' USING PigStorage(',');\n", pathToUdf);

        String expectedSQL = "SELECT SUBSTRING(`PIG_TUPLE`(`name`, 0, 3)) AS `short_name`, `age`\n" +
                "FROM `data`.`txt`";

        String result = commands.convertPigStringToSQL(pigScript);
        assertEquals(expectedSQL, result.trim());
    }

    @Test
    void testConvertPigStringToSQLWithSHAUDF() {
        String pathToDataFu = Paths.get("libs", "datafu-pig-1.6.1.jar").toAbsolutePath().toString();

        String pigScript = String.format(
                "REGISTER '%s';\n" +
                        "define SHA datafu.pig.hash.SHA();\n" +
                        "data_in = LOAD 'input' as (val:chararray);\n" +
                        "data_out = FOREACH data_in GENERATE SHA(val) as val;\n",
                pathToDataFu
        );

        String expectedSQL = "SELECT `SHA`(`PIG_TUPLE`(`val`)) AS `val`\n" +
                "FROM `input`";

        String result = commands.convertPigStringToSQL(pigScript);
        assertEquals(expectedSQL, result.trim());
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
