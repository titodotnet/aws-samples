namespace AWSSampleConsoleApp1.BlobToS3
{
    using Amazon.S3;
    using Microsoft.WindowsAzure.Storage.Blob;

    /// <summary>
    /// Holds the details of BLOB to S3 request details.
    /// </summary>
    public class BlobToS3Request
    {
        /// <summary>
        /// Gets or sets the Blob Client.
        /// </summary>
        public CloudBlobClient BlobClient { get; set; }

        /// <summary>
        /// Gets or sets the source BLOB container.
        /// </summary>
        public string SourceBlobContainer { get; set; }

        /// <summary>
        /// Gets or sets the source BLOB.
        /// </summary>
        public string SourceBlob { get; set; }

        /// <summary>
        /// Gets or sets the S3 client.
        /// </summary>
        public AmazonS3Client S3Client { get; set; }

        /// <summary>
        /// Gets or sets the target S3 bucket.
        /// </summary>
        public string TargetS3Bucket { get; set; }

        /// <summary>
        /// Gets or sets the target S3 file.
        /// </summary>
        public string TargetS3File { get; set; }
    }
}
