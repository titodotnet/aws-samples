using Amazon.S3;
using Amazon.S3.Model;
using Microsoft.Azure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;

namespace AWSSampleConsoleApp1
{
    public class UploadLargeFileAsChunk
    {
        public static void Main()
        {
            var largeFileProcessor = new LargeFileProcessor();
            largeFileProcessor.CopyLargeFileFromAzureBlobToAwsS3().Wait();

            Console.ReadKey();
        }
    }

    public class LargeFileProcessor
    {
        /// <summary>
        /// Part size to read from BLOB and upload to S3.
        /// </summary>
        private const long PartSize = 104857600; // 100 MB.

        /// <summary>
        /// AWS Acsess Id.
        /// </summary>
        private string AwsAccessKeyId => CloudConfigurationManager.GetSetting("AwsAccessKeyId");

        /// <summary>
        /// AWS Secret Key.
        /// </summary>
        private string AwsSecretKey => CloudConfigurationManager.GetSetting("AwsSecretKey");

        /// <summary>
        /// S3 bucket name.
        /// </summary>
        private string AwsS3BucketName => CloudConfigurationManager.GetSetting("AwsS3BucketName");

        /// <summary>
        /// S3 file name to be copied to.
        /// </summary>
        private string TargetFileName => CloudConfigurationManager.GetSetting("TargetFileName");

        /// <summary>
        /// Azure storage account.
        /// </summary>
        private string StorageAccount => CloudConfigurationManager.GetSetting("StorageAccount");

        /// <summary>
        /// Azure BLOB container name.
        /// </summary>
        private string ContainerName => CloudConfigurationManager.GetSetting("ContainerName");

        /// <summary>
        /// Azure BLOB file name to be copied.
        /// </summary>
        private string BlobFileName => CloudConfigurationManager.GetSetting("BlobFileName");

        /// <summary>
        /// Logger instance.
        /// </summary>
        private ILogger logger = new Logger();

        /// <summary>
        /// Copies large file as chunks from Azure BLOB to Amazon S3.
        /// </summary>
        /// <returns></returns>
        public async Task CopyLargeFileFromAzureBlobToAwsS3()
        {
            AmazonS3Client s3Client = new AmazonS3Client(AwsAccessKeyId, AwsSecretKey, Amazon.RegionEndpoint.APSouth1);

            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(StorageAccount); //Create Storage account reference.
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient(); // Create the blob client.
            CloudBlobContainer container = blobClient.GetContainerReference(ContainerName); // Retrieve reference to a container.

            container.CreateIfNotExists();

            CloudBlockBlob blob = container.GetBlockBlobReference(BlobFileName); // Create Blob reference.

            blob.FetchAttributes(); // Prepare blob instance To get the file length.

            var remainingBytes = blob.Properties.Length;
            long readPosition = 0; // To be used offset / position from where to start reading from BLOB.

            InitiateMultipartUploadRequest initiateMultipartUploadRequest = new InitiateMultipartUploadRequest
            {
                BucketName = AwsS3BucketName,
                Key = TargetFileName
            };

            // Will use UploadId from this response.
            InitiateMultipartUploadResponse initiateMultipartUploadResponse = s3Client.InitiateMultipartUpload(initiateMultipartUploadRequest); 
            List<UploadPartResponse> uploadPartResponses = new List<UploadPartResponse>();

            Stopwatch stopwatch = Stopwatch.StartNew();

            try
            {
                int partCounter = 0; // To increment on each read of parts and use it as part number.

                while (remainingBytes > 0)
                {
                    // Determine the size when final block reached as it might be less than Part size. 
                    // Will be PartSize except final block.
                    long bytesToCopy = Math.Min(PartSize, remainingBytes); 

                    using (MemoryStream memoryStream = new MemoryStream())
                    {
                        // To download part from BLOB.
                        await blob.DownloadRangeToStreamAsync(memoryStream, readPosition, bytesToCopy).ConfigureAwait(false);
                        memoryStream.Position = 0;
                        partCounter++;

                        UploadPartRequest uploadRequest = new UploadPartRequest
                        {
                            BucketName = AwsS3BucketName,
                            Key = TargetFileName,
                            UploadId = initiateMultipartUploadResponse.UploadId,
                            PartNumber = partCounter,
                            PartSize = bytesToCopy,
                            InputStream = memoryStream
                        };

                        UploadPartResponse uploadPartResponse = s3Client.UploadPart(uploadRequest);
                        uploadPartResponses.Add(uploadPartResponse);

                        remainingBytes -= bytesToCopy;
                        readPosition += bytesToCopy;

                        this.logger.WriteLine($"Uploaded part with part number {partCounter}, size {bytesToCopy}bytes and remaining {remainingBytes}bytes to read.");
                    }
                }

                CompleteMultipartUploadRequest completeMultipartUploadRequest = new CompleteMultipartUploadRequest
                {
                    BucketName = AwsS3BucketName,
                    Key = TargetFileName,
                    UploadId = initiateMultipartUploadResponse.UploadId
                };

                completeMultipartUploadRequest.AddPartETags(uploadPartResponses);

                CompleteMultipartUploadResponse completeMultipartUploadResponse = await s3Client.CompleteMultipartUploadAsync(completeMultipartUploadRequest).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                this.logger.WriteLine($"Exception : {exception.Message}");
                AbortMultipartUploadRequest abortMultipartUploadRequest = new AbortMultipartUploadRequest
                {
                    BucketName = AwsS3BucketName,
                    Key = TargetFileName,
                    UploadId = initiateMultipartUploadResponse.UploadId
                };

                await s3Client.AbortMultipartUploadAsync(abortMultipartUploadRequest).ConfigureAwait(false);
            }
            finally
            {
                stopwatch.Stop();
                this.logger.WriteLine($"Execution time in mins: {stopwatch.Elapsed.TotalMinutes}");
            }
        }
    }
}
