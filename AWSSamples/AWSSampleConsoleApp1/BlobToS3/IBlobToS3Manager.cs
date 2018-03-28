namespace AWSSampleConsoleApp1.BlobToS3
{
    using System.Threading.Tasks;

    public interface IBlobToS3Manager
    {
        /// <summary>
        /// Copies from BLOB to s3.
        /// </summary>
        /// <returns>Returns the result of copy from BLOB to S3.</returns>
        Task<BlobToS3Response> CopyFromBlobToS3Async();
    }
}
