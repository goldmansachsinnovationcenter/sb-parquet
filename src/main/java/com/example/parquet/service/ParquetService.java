package com.example.parquet.service;

import com.example.parquet.model.ServiceRecord;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.springframework.stereotype.Service;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

@Service
public class ParquetService {

    /**
     * Generates a parquet file from the provided data map and saves it to the specified file path.
     * Then reads the file back to verify it was written correctly.
     * Also stores input content and output content to separate files for verification.
     *
     * @param data     Map containing the service record data with keys: id, name, serviceName, status
     * @param filePath Path where the parquet file should be saved
     */
    public void generateParquetFile(Map<String, String> data, String filePath) {
        try {
            String baseDir = new File("").getAbsolutePath();
            String inputFilePath = baseDir + "/output/input_content.json";
            storeInputContent(data, inputFilePath);
            
            ServiceRecord record = ServiceRecord.newBuilder()
                    .setId(data.get("id"))
                    .setName(data.get("name"))
                    .setServiceName(data.get("serviceName"))
                    .setStatus(data.get("status"))
                    .build();

            File outputFile = new File(filePath);
            if (outputFile.getParentFile() != null && !outputFile.getParentFile().exists()) {
                outputFile.getParentFile().mkdirs();
            }

            if (outputFile.exists()) {
                outputFile.delete();
            }

            writeToParquetFile(record, filePath);

            String outputFilePath = baseDir + "/output/parquet_read_output.txt";
            readAndStoreParquetFile(filePath, outputFilePath);

        } catch (Exception e) {
            System.err.println("Error generating Parquet file: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Stores the input Map content to a JSON file.
     *
     * @param data     Map containing the input data
     * @param filePath Path where to save the input content
     * @throws IOException If an I/O error occurs
     */
    private void storeInputContent(Map<String, String> data, String filePath) throws IOException {
        File file = new File(filePath);
        if (file.getParentFile() != null && !file.getParentFile().exists()) {
            file.getParentFile().mkdirs();
        }
        
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
            writer.write("{\n");
            writer.write("  \"id\": \"" + data.get("id") + "\",\n");
            writer.write("  \"name\": \"" + data.get("name") + "\",\n");
            writer.write("  \"serviceName\": \"" + data.get("serviceName") + "\",\n");
            writer.write("  \"status\": \"" + data.get("status") + "\"\n");
            writer.write("}\n");
            System.out.println("Input content stored to file: " + filePath);
        }
    }

    /**
     * Writes a ServiceRecord to a Parquet file.
     *
     * @param record   The ServiceRecord to write
     * @param filePath The path where the Parquet file should be saved
     * @throws IOException If an I/O error occurs
     */
    private void writeToParquetFile(ServiceRecord record, String filePath) throws IOException {
        Path path = new Path(filePath);
        
        try (ParquetWriter<ServiceRecord> writer = AvroParquetWriter.<ServiceRecord>builder(path)
                .withSchema(record.getSchema())
                .withConf(new Configuration())
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build()) {
            
            writer.write(record);
            System.out.println("Successfully wrote record to Parquet file: " + filePath);
        }
    }

    /**
     * Reads a Parquet file and stores its content to an output file.
     *
     * @param parquetFilePath The path of the Parquet file to read
     * @param outputFilePath  The path where to save the output content
     * @throws IOException If an I/O error occurs
     */
    private void readAndStoreParquetFile(String parquetFilePath, String outputFilePath) throws IOException {
        Path path = new Path(parquetFilePath);
        
        File outputFile = new File(outputFilePath);
        if (outputFile.getParentFile() != null && !outputFile.getParentFile().exists()) {
            outputFile.getParentFile().mkdirs();
        }
        
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));
             ParquetReader<ServiceRecord> reader = AvroParquetReader
                     .<ServiceRecord>builder(path)
                     .withConf(new Configuration())
                     .build()) {
            
            writer.write("Reading Parquet file content from: " + parquetFilePath + "\n");
            System.out.println("Reading Parquet file content from: " + parquetFilePath);
            
            ServiceRecord record;
            while ((record = reader.read()) != null) {
                writer.write("Record content:\n");
                writer.write("  id: " + record.getId() + "\n");
                writer.write("  name: " + record.getName() + "\n");
                writer.write("  serviceName: " + record.getServiceName() + "\n");
                writer.write("  status: " + record.getStatus() + "\n");
                
                System.out.println("Record content:");
                System.out.println("  id: " + record.getId());
                System.out.println("  name: " + record.getName());
                System.out.println("  serviceName: " + record.getServiceName());
                System.out.println("  status: " + record.getStatus());
            }
            
            System.out.println("Parquet read output stored to file: " + outputFilePath);
        }
    }
}
