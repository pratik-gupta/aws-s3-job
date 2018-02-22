package com.pearson.aws.job;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Date;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.amazonaws.AmazonClientException;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;

@Component
public class S3BucketCRUDJob {

	AmazonS3 amazonS3;

	{
		amazonS3 = new AmazonS3Client();
		Region region = Region.getRegion(Regions.US_EAST_1);
		amazonS3.setRegion(region);
	}		
	
	@Scheduled(fixedRate = 30000)
	public void S3CrudOperatios() {
		
		String bucketName= "bucket-" + new Date().getTime();
		String folderName= "folder-" + new Date().getTime();
		String key = "MyObject";
		try {
			System.out.println("-- Creating Bucket :"+bucketName +" with folder "+ folderName);
			amazonS3.createBucket(bucketName);
			
			System.out.println("-- Listing Buckets --");
			for(Bucket bucket: amazonS3.listBuckets()) {
				System.out.println(" - " + bucket.getName());
			}
			System.out.println();
			
			System.out.println(" -- Uploading a new object in folder :"+ folderName);
            amazonS3.putObject(new PutObjectRequest(bucketName, folderName+"/"+key, createSampleFile()));
            
            System.out.println();
            System.out.println(" -- Listing objects --");
            ObjectListing objectListing = amazonS3.listObjects(new ListObjectsRequest()
                    .withBucketName(bucketName));
            for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
                System.out.println(" - " + objectSummary.getKey() + "  " +
                        "(size = " + objectSummary.getSize() + ")");
            }
            System.out.println();
            
            System.out.println(" -- Deleting an object " +key + "\n");
            amazonS3.deleteObject(bucketName, folderName+"/"+key);

            System.out.println(" -- Deleting bucket " + bucketName + "\n");
            amazonS3.deleteBucket(bucketName);
            
		} catch (AmazonClientException | IOException ex) {
			ex.printStackTrace();
		}
				
	}
	
    private static File createSampleFile() throws IOException {
        File file = File.createTempFile("sample-object", ".txt");
        file.deleteOnExit();

        Writer writer = new OutputStreamWriter(new FileOutputStream(file));
        writer.write("This is a test file from aws java sdk");
        writer.close();

        return file;
    }
}
