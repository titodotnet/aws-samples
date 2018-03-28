namespace AWSSampleConsoleApp1.BlobToS3
{
    using Amazon.S3.Model;
    using Microsoft.WindowsAzure.Storage.Blob;
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Security.Cryptography;
    using System.Threading.Tasks;

    /// <summary>
    /// Handles the BLOB to S3 
    /// </summary>
    public class BlobToS3Manager : IBlobToS3Manager
    {
        /// <summary>
        /// The container not exists.
        /// </summary>
        private const string ContainerNotExists = "BLOB Container doesn't exists.";

        /// <summary>
        /// The BLOB not exists.
        /// </summary>
        private const string BlobNotExists = "BLOB doesn't exists.";

        /// <summary>
        /// Part size to read from BLOB and upload to S3.
        /// </summary>
        private const long PartSize = 104857600; // 100 MB.

        /// <summary>
        /// The BLOB S3 request.
        /// </summary>
        private readonly BlobToS3Request blobToS3Request;
        /// <summary>
        /// Initializes new instance of BLOB to S3 Manager.
        /// </summary>
        /// <param name="blobToS3Request">The BLOB to S3 request.</param>
        public BlobToS3Manager(BlobToS3Request blobToS3Request)
        {
            this.blobToS3Request = blobToS3Request;
        }

        /// <summary>
        /// Copies BLOB to S3.
        /// </summary>
        /// <returns>The BLOB to S3 response.</returns>
        public async Task<BlobToS3Response> CopyFromBlobToS3Async()
        {
            BlobToS3Response blobToS3Response = new BlobToS3Response();
            var validation = await this.Validate();

            if (!validation.Item1)
            {
                return validation.Item2;
            }

            var sourceBlob = validation.Item3;
            await sourceBlob.FetchAttributesAsync();
            var remainingBytes = sourceBlob.Properties.Length;
            long readPosition = 0; // To be used offset / position from where to start reading from BLOB.

            InitiateMultipartUploadRequest initiateMultipartUploadRequest = new InitiateMultipartUploadRequest
            {
                BucketName = this.blobToS3Request.TargetS3Bucket,
                Key = this.blobToS3Request.TargetS3File
            };

            // Will use UploadId from this response.
            InitiateMultipartUploadResponse initiateMultipartUploadResponse = this.blobToS3Request.S3Client.InitiateMultipartUpload(initiateMultipartUploadRequest);
            List<UploadPartResponse> uploadPartResponses = new List<UploadPartResponse>();

            try
            {
                int partCounter = 0; // To increment on each read of parts and use it as part number.
                var sha256 = new SHA256Managed();

                while (remainingBytes > 0)
                {
                    // Determine the size when final block reached as it might be less than Part size. 
                    // Will be PartSize except final block.
                    long bytesToCopy = Math.Min(PartSize, remainingBytes);

                    using (MemoryStream memoryStream = new MemoryStream())
                    {
                        // To download part from BLOB.
                        await sourceBlob.DownloadRangeToStreamAsync(memoryStream, readPosition, bytesToCopy).ConfigureAwait(false);
                        memoryStream.Position = 0;
                        partCounter++;

                        UploadPartRequest uploadRequest = new UploadPartRequest
                        {
                            BucketName = this.blobToS3Request.TargetS3Bucket,
                            Key = this.blobToS3Request.TargetS3File,
                            UploadId = initiateMultipartUploadResponse.UploadId,
                            PartNumber = partCounter,
                            PartSize = bytesToCopy,
                            InputStream = memoryStream
                        };

                        UploadPartResponse uploadPartResponse = this.blobToS3Request.S3Client.UploadPart(uploadRequest);
                        uploadPartResponses.Add(uploadPartResponse);

                        remainingBytes -= bytesToCopy;
                        readPosition += bytesToCopy;

                        // $"Uploaded part with part number {partCounter}, size {bytesToCopy}bytes and remaining {remainingBytes}bytes to read.")

                        // Calculate the checksum value.
                        if (remainingBytes <= 0)
                        {
                            sha256.TransformFinalBlock(memoryStream.ToArray(), 0, (int)bytesToCopy);
                        }
                        else
                        {
                            byte[] bytesToSend = memoryStream.ToArray();
                            sha256.TransformBlock(bytesToSend, 0, (int)bytesToCopy, bytesToSend, 0);
                        }
                    }
                }

                blobToS3Response.Sha256CheckSum = BitConverter.ToString(sha256.Hash).Replace("-", string.Empty);

                CompleteMultipartUploadRequest completeMultipartUploadRequest = new CompleteMultipartUploadRequest
                {
                    BucketName = this.blobToS3Request.TargetS3Bucket,
                    Key = this.blobToS3Request.TargetS3File,
                    UploadId = initiateMultipartUploadResponse.UploadId
                };

                completeMultipartUploadRequest.AddPartETags(uploadPartResponses);

                CompleteMultipartUploadResponse completeMultipartUploadResponse = await this.blobToS3Request.S3Client.CompleteMultipartUploadAsync(completeMultipartUploadRequest).ConfigureAwait(false);

                blobToS3Response.IsSuccess = true;
                blobToS3Response.S3Path = completeMultipartUploadResponse.Location;
            }
            catch (Exception exception)
            {
                blobToS3Response.IsSuccess = false;
                blobToS3Response.Message = exception.Message;

                AbortMultipartUploadRequest abortMultipartUploadRequest = new AbortMultipartUploadRequest
                {
                    BucketName = this.blobToS3Request.TargetS3Bucket,
                    Key = this.blobToS3Request.TargetS3File,
                    UploadId = initiateMultipartUploadResponse.UploadId
                };

                await this.blobToS3Request.S3Client.AbortMultipartUploadAsync(abortMultipartUploadRequest).ConfigureAwait(false);
            }

            return blobToS3Response;
        }

        /// <summary>
        /// Validates the source BLOB is valid.
        /// </summary>
        /// <returns>The resultant tuple.</returns>
        private async Task<Tuple<bool, BlobToS3Response, CloudBlockBlob>> Validate()
        {
            BlobToS3Response blobToS3Response = null;
            CloudBlobContainer cloudBlobContainer = this.blobToS3Request.BlobClient.GetContainerReference(this.blobToS3Request.SourceBlobContainer);

            if (!await cloudBlobContainer.ExistsAsync())
            {
                blobToS3Response = new BlobToS3Response
                {
                    IsSuccess = false,
                    Message = ContainerNotExists
                };

                return new Tuple<bool, BlobToS3Response, CloudBlockBlob>(false, blobToS3Response, null);
            }

            CloudBlockBlob cloudBlockBlob = cloudBlobContainer.GetBlockBlobReference(this.blobToS3Request.SourceBlob);

            if (await cloudBlockBlob.ExistsAsync())
            {
                return new Tuple<bool, BlobToS3Response, CloudBlockBlob>(true, null, cloudBlockBlob);
            }

            blobToS3Response = new BlobToS3Response
            {
                IsSuccess = false,
                Message = BlobNotExists
            };

            return new Tuple<bool, BlobToS3Response, CloudBlockBlob>(false, blobToS3Response, null);
        }

    }
}
