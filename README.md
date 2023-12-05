# ConvertPigToSQL

## Overview

ConvertPigToSQL is a tool designed to explore the capabilities of the Apache Calcite library in converting Apache Pig scripts into SQL.
Inspired by the [pig2sql](https://github.com/dstreev/pig2sql) repository, this project serves as a straightforward solution for those looking to transform existing Pig scripts into SQL queries.

This README provides an overview of how to use the tool, how to generate a standalone shadow JAR that bundles all dependencies, and how to work with the classic JAR.

For more information and resources on Apache Calcite and its capabilities, visit the official [Apache Calcite website](https://calcite.apache.org/) and [Apache Calcite Github](https://github.com/apache/calcite)

## Features

Converts Pig scripts to SQL.
Supports conversion from a single Pig string, a file, or an entire directory containing multiple Pig files.
Utilizes Apache Calcite for the conversion process.

## Building the JAR

### Classic JAR

To build the classic JAR (without bundled dependencies), run:

```
./gradlew jar
```

This will produce a JAR file in the `build/libs` directory.


### Shadow JAR

If you need to build a shadow JAR (a JAR that includes all dependencies), you can use the provided Gradle script.

To build the shadow JAR, run:

```
./gradlew shadowJar
```

This will produce a JAR file in the build/libs directory with a name like convertPigToSql-1.0.jar.

## Setting Permissions for the Batch File

Before running the `pigToSql` batch file on UNIX-based systems, you may need to give it execute permissions. You can do this with the following command:

```
chmod +x pigtosql
```

This ensures that the batch file can be run as an executable script.


## Usage

Run the compiled JAR with the appropriate options.

### Using the Batch File

You can run the pigToSql tool using the provided batch file. Here's how to use it:
```
./pigToSql [options]
```

#### Available Options:

* -s, --string <pigString>: String containing the Pig script to convert.
* -i, --inputFile <inputFilePath>: Path to the Pig file to convert.
* -o, --outputFile <outputFilePath>: Path to the output SQL file.
* -id, --inputDirectory <inputDirectoryPath>: Directory containing Pig files to convert.
* -od, --outputDirectory <outputDirectoryPath>: Directory to store the converted SQL files.

For instance, if you have a Pig script named `test.pig` and you want to convert it to SQL and save the result in `output.sql`, you would use the following command:

```
./pigToSql -i test.pig -o output.sql

```

#### Using the JAR Directly

If you prefer to use the JAR directly, you can do so with the following command:

```
java -jar path_to_your_jar.jar [options]
```

Replace path_to_your_jar.jar with the path to your built JAR file (build/libs), and [options] with the appropriate command-line options as mentioned above.

## Contributing

Contributions are welcome. Please open an issue or submit a pull request.



