package com.example.parquet.controller;

import com.example.parquet.service.ParquetService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/parquet")
public class ParquetController {

    private final ParquetService parquetService;

    @Autowired
    public ParquetController(ParquetService parquetService) {
        this.parquetService = parquetService;
    }

    @PostMapping("/generate")
    public ResponseEntity<String> generateParquetFile(@RequestBody Map<String, String> data) {
        try {
            if (!data.containsKey("id") || !data.containsKey("name") || 
                !data.containsKey("serviceName") || !data.containsKey("status")) {
                return ResponseEntity.badRequest().body("Missing required fields. Required: id, name, serviceName, status");
            }
            
            String filePath = "output/service_record.parquet";
            parquetService.generateParquetFile(data, filePath);
            
            return ResponseEntity.ok("Parquet file generated successfully at: " + filePath);
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body("Error generating Parquet file: " + e.getMessage());
        }
    }
}
