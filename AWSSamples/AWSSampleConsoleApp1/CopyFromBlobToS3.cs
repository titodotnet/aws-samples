using Amazon.S3;
using AWSSampleConsoleApp1.BlobToS3;
using Microsoft.Azure;
using Microsoft.WindowsAzure.Storage;
using System;
using System.Threading.Tasks;

namespace AWSSampleConsoleApp1
{
    public class CopyFromBlobToS3
    {
        public static void Main()
        {
            CopyFromBlobToS3Client copyFromBlobToS3Client = new CopyFromBlobToS3Client();
            BlobToS3Response blobToS3Response = copyFromBlobToS3Client.ProcessAsync().Result;

            Console.WriteLine($"Is copy from BLOB to S3 successfull: {blobToS3Response.IsSuccess}");
            if (blobToS3Response.IsSuccess)
            {
                Console.WriteLine($"S3 Path: {blobToS3Response.S3Path}");
                Console.WriteLine($"SHA256 cheksum of uploaded file: {blobToS3Response.Sha256CheckSum}");
            }
            else
            {
                Console.WriteLine($"Failure reason: {blobToS3Response.Message}");
            }            
        }        
    }

    public class CopyFromBlobToS3Client
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

        public async Task<BlobToS3Response> ProcessAsync()
        {
            BlobToS3Request blobToS3Request = new BlobToS3Request
            {
                BlobClient = CloudStorageAccount.Parse(StorageAccount).CreateCloudBlobClient(),
                SourceBlob = BlobFileName,
                SourceBlobContainer = ContainerName,
                S3Client = new AmazonS3Client(AwsAccessKeyId, AwsSecretKey, Amazon.RegionEndpoint.APSouth1),
                TargetS3Bucket = AwsS3BucketName,
                TargetS3File = TargetFileName
            };

            IBlobToS3Manager blobToS3Manager = new BlobToS3Manager(blobToS3Request);
            return await blobToS3Manager.CopyFromBlobToS3Async();
        }
    }


}
