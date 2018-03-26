namespace AWSSampleConsoleApp1.BlobToS3
{
    /// <summary>
    /// Holds the details of BLOB to S3 response details.
    /// </summary>
    public class BlobToS3Response
    {
        /// <summary>
        /// Gets or sets a value indicating whether copy operation is success.
        /// </summary>
        public bool IsSuccess { get; set; }

        /// <summary>
        /// Gets or sets the S3 path.
        /// </summary>
        public string S3Path { get; set; }

        /// <summary>
        /// Gets or sets the message.
        /// </summary>
        public string Message { get; set; }

        /// <summary>
        /// Gets or sets the SHA256 check sum.
        /// </summary>
        public string Sha256CheckSum { get; set; }
    }
}
