package com.example.kafka;
import java.io.File;  // Import the File class
import java.io.IOException;  // Import the IOException class to handle errors
import java.io.FileWriter;
import java.io.FileWriter;

import java.io.File;

//import software.amazon.awssdk.core.sync.RequestBody;
//import software.amazon.awssdk.services.s3.S3Client;
//import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public class CreateFile {

    public static void main(String[] args) throws IOException {


//        FileWriter writer = new FileWriter("/Users/Wolverine/Desktop/demo.txt");
        FileWriter writer = new FileWriter("filename2.txt");

        String arr[] = { "ONE", "TWO", "THREE", "FOUR", "FIVE", "SIX", "SEVEN", "EIGHT", "NINE" };
        int len = arr.length;
        for (int i = 0; i < len; i++) {
            writer.write(arr[i]  + System.lineSeparator());

        }
        writer.close();

    }
}
