using Amazon;
using Amazon.S3;
using Amazon.S3.Model;
using System;
using System.Configuration;
using System.IO;
using System.Text;

namespace Microsoft.Practices.EnterpriseLibrary.SemanticLogging.Sinks
{
    public class AmazonS3Sink : IObserver<EventEntry>
    {
        #region Fields

        private const string BucketNameSettingsKey = "BucketName";
        private const string FileNameSettingsKey = "FileName";

        private readonly IAmazonS3 _s3Client;
        private readonly StringWriter _stringWriter;
        private readonly string _fileName;
        private readonly string _bucketName;
        
        #endregion Fields
        public AmazonS3Sink()
        {
            _stringWriter = new StringWriter(new StringBuilder());

            _s3Client = AWSClientFactory.CreateAmazonS3Client();

            _bucketName = ConfigurationManager.AppSettings.Get(BucketNameSettingsKey);
            _fileName = ConfigurationManager.AppSettings.Get(FileNameSettingsKey);

            var request = new GetObjectRequest { BucketName = _bucketName, Key = _fileName };

            using (var obj = _s3Client.GetObject(request))
            {
                using (StreamReader reader = new StreamReader(obj.ResponseStream))
                {
                    _stringWriter.WriteLine(reader.ReadToEnd());
                }
            }
        }

        #region IObserver Implementation

        public void OnCompleted()
        {
            _stringWriter.Flush();

            var putRequest = new PutObjectRequest()
                                    {
                                        Key = _fileName,
                                        BucketName = _bucketName,
                                        InputStream = new MemoryStream(Encoding.ASCII.GetBytes(_stringWriter.GetStringBuilder().ToString()))
                                    };

            _s3Client.PutObject(putRequest);
        }

        public void OnError(Exception error)
        {
            _stringWriter.Flush();
        }

        public void OnNext(EventEntry value)
        {
            _stringWriter.WriteLine(value.FormattedMessage);
        }

        #endregion IObserver Implementation
    }
}
