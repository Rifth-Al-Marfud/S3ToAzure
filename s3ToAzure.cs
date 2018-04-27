using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Transfer;
using Amazon.S3.Util;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Auth;
using System.Globalization;
 
namespace CopyAmazonBucketToBlobStorage {
    class Program {
        // Windows Azure Storage Account Name
        private static string azureStorageAccountName = "awsusage";
        // Windows Azure Storage Account Key
        private static string azureStorageAccountKey = "XXJRWb7JQT1bSi/Y7HmsvdNOdHs0VjoRCGU2bk8E125jJWqrwLX1/cBkhUJzeWpHjlloiTcusJWvxcDDGkSPlQ==";
        // Windows Azure Blob Container Name (Target)
        private static string azureBlobContainerName = "aws-cloudtrail";
        // Amazon Access Key
        private static string amazonAccessKey = "AKIAJY46UDEJ52OYGIIQ";
        // Amazon Secret Key
        private static string amazonSecretKey = "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";
        // Amazon Bucket Name (Source)
        private static string amazonBucketName = "crc-rifth";
        private static string objectUrlFormat = "https://s3.amazonaws.com/{0}/{1}";
        
        // This dictionary will keep track of progress. The key would be the URL of the blob.
        private static Dictionary<string, CopyBlobProgress> CopyBlobProgress;
 
        static void Main(string[] args) {
            // Create a reference of Windows Azure Storage Account
            StorageCredentials azureCloudStorageCredentials = new StorageCredentials(azureStorageAccountName, azureStorageAccountKey);
            CloudStorageAccount azureCloudStorageAccount = new CloudStorageAccount(azureCloudStorageCredentials, true);
            // Get a reference of Blob Container where the objects will be copied
            var blobContainer = azureCloudStorageAccount.CreateCloudBlobClient().GetContainerReference(azureBlobContainerName);
            // Create blob container if needed
            Console.WriteLine("Searching for the \"" + azureBlobContainerName + "\" blob container...");
            if (blobContainer.CreateIfNotExists())
                Console.WriteLine("The \"" + azureBlobContainerName + "\" blob container was not found.\nThe container has now been created.");
            else
                Console.WriteLine("The \"" + azureBlobContainerName + "\" blob container was found.");
            Console.WriteLine();
            // Create a reference of Amazon Client
            AmazonS3Client amazonClient = new AmazonS3Client(amazonAccessKey, amazonSecretKey, Amazon.RegionEndpoint.USEast1);
            // Initialize dictionary
            CopyBlobProgress = new Dictionary<string, CopyBlobProgress>();

            string[] months = { "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec" };
            DateTime today = DateTime.Now;
            string todayString = today.ToString("d");
            string[] dateParsed = todayString.Split('/');
            int day = Int32.Parse(dateParsed[1]);
            int month;
            if (Int32.Parse(dateParsed[0]) >= 2)
                month = Int32.Parse(dateParsed[0]) - 2;
            else
                month = Int32.Parse(dateParsed[0]) + 10;
            int year;
            if (day == 1 && Int32.Parse(dateParsed[0]) == 1)
                year = Int32.Parse(dateParsed[2]) - 1;
            else
                year = Int32.Parse(dateParsed[2]);

            string continuationToken = null;
            bool continueListObjects = true;
            // Since ListObjects returns a maximum of 1000 objects in a single call,
            // the function will have to be called repeatedly until all objects are returned.
            while (continueListObjects) {
                ListObjectsRequest requestToListObjects = new ListObjectsRequest() {
                    BucketName = amazonBucketName,
                    Marker = continuationToken,
                    MaxKeys = 100,
                };
                Console.WriteLine("Attempting to list objects from the \"" + amazonBucketName + "\" S3 AWS bucket...");
                // List objects from Amazon S3
                var listObjectsResponse = amazonClient.ListObjects(requestToListObjects);
                // Get the list of objects
                var objectsFetchedSoFar = listObjectsResponse.S3Objects;
                Console.WriteLine("Object listing complete. Now beginning the copy process...");
                Console.WriteLine();
                // See if more objects are available. We'll first process the objects fetched
                // and continue the loop to get next set of objects.
                continuationToken = listObjectsResponse.NextMarker;
                foreach (var s3Object in objectsFetchedSoFar) {
                    string objectKey = s3Object.Key;
                    //if (day == 6 && objectKey.Contains("AWS-") && objectKey.Contains(months[month]) && objectKey.Contains(dateParsed[2])) { }
                    //else if (objectKey.Contains("AWS-") && objectKey.Length == 35) continue;
                    if (day == 1 && !objectKey.Contains("CloudTrail-" + months[month] + "-" + dateParsed[2])) continue;
                    else if (day != 1) {
                        if (!objectKey.Contains("CloudTrail-" + months[month + 1] + "-" + dateParsed[2])) continue;
                    }
                    // Since ListObjects returns both files and folders, for now we'll skip folders
                    // The way we'll check this is by checking the value of S3 Object Key. If it
                    // end with "/" we'll assume it's a folder.
                    if (objectKey.Contains("CloudTrail-Logs-Final")) {
                        // Construct the URL to copy
                        string urlToCopy = string.Format(CultureInfo.InvariantCulture, objectUrlFormat, amazonBucketName, objectKey);
                        // Create an instance of CloudBlockBlob
                        string azureObjectKey = objectKey.Replace("CloudTrail-Logs-Final/", "");
                        CloudBlockBlob blockBlob = blobContainer.GetBlockBlobReference(azureObjectKey);
                        var blockBlobUrl = blockBlob.Uri.AbsoluteUri;
                        if (!CopyBlobProgress.ContainsKey(blockBlobUrl)) {
                            CopyBlobProgress.Add(blockBlobUrl, new CopyBlobProgress() {
                                    Status = CopyStatus.NotStarted,
                            });
                            // Start the copy process. We would need to put it in try/catch block
                            // as the copy from Amazon S3 will only work with publicly accessible objects.
                            try {
                                Console.WriteLine(string.Format("Attempting to copy from \"{0}\" to \"{1}\"...", urlToCopy, blockBlobUrl));
                                GetPreSignedUrlRequest request1 = new GetPreSignedUrlRequest() {
                                    BucketName = amazonBucketName,
                                    Key = objectKey,
                                    Expires = DateTime.Now.AddMinutes(1)
                                };
                                urlToCopy = amazonClient.GetPreSignedURL(request1);
                                blockBlob.StartCopy(new Uri(urlToCopy));
                                CopyBlobProgress[blockBlobUrl].Status = CopyStatus.Started;
                            } catch (Exception exception) {
                                CopyBlobProgress[blockBlobUrl].Status = CopyStatus.Failed;
                                CopyBlobProgress[blockBlobUrl].Error = exception;
                            }
                        }
                    }
                }
                Console.WriteLine();
                Console.WriteLine("-");
                Console.WriteLine();
                Console.WriteLine("Checking the status of copy processes...");

                Microsoft.WindowsAzure.Storage.Blob.CopyStatus[] tracker = new Microsoft.WindowsAzure.Storage.Blob.CopyStatus[1000];
                // Now track the progress
                bool checkForBlobCopyStatus = true;
                while (checkForBlobCopyStatus) {
                    // List blobs in the blob container
                    var blobsList = blobContainer.ListBlobs("", true, BlobListingDetails.Copy);
                    int x = 0;
                    bool written = false;
                    foreach (var blob in blobsList) {
                        var tempBlockBlob = blob as CloudBlob;
                        var copyStatus = tempBlockBlob.CopyState;
                        if (CopyBlobProgress.ContainsKey(tempBlockBlob.Uri.AbsoluteUri)) {
                            var copyBlobProgress = CopyBlobProgress[tempBlockBlob.Uri.AbsoluteUri];
                            if (copyStatus != null && !copyStatus.Status.Equals(tracker[x])) {
                                tracker[x] = copyStatus.Status;
                                Console.Write(string.Format("Status of \"{0}\" blob copy: ", tempBlockBlob.Uri.AbsoluteUri, copyStatus.Status));
                                switch (copyStatus.Status)
                                {
                                    case Microsoft.WindowsAzure.Storage.Blob.CopyStatus.Aborted:
                                        if (copyBlobProgress != null) {
                                            copyBlobProgress.Status = CopyStatus.Aborted;
                                            Console.WriteLine("Aborted!");
                                        }
                                        break;
                                    case Microsoft.WindowsAzure.Storage.Blob.CopyStatus.Failed:
                                        if (copyBlobProgress != null) {
                                            copyBlobProgress.Status = CopyStatus.Failed;
                                            Console.WriteLine("Failed!");
                                        }
                                        break;
                                    case Microsoft.WindowsAzure.Storage.Blob.CopyStatus.Invalid:
                                        if (copyBlobProgress != null) {
                                            copyBlobProgress.Status = CopyStatus.Invalid;
                                            Console.WriteLine("Invalid!");
                                        }
                                        break;
                                    case Microsoft.WindowsAzure.Storage.Blob.CopyStatus.Pending:
                                        if (copyBlobProgress != null) {
                                            copyBlobProgress.Status = CopyStatus.Pending;
                                            Console.WriteLine("Pending...");
                                        }
                                        break;
                                    case Microsoft.WindowsAzure.Storage.Blob.CopyStatus.Success:
                                        if (copyBlobProgress != null) {
                                            copyBlobProgress.Status = CopyStatus.Success;
                                            Console.WriteLine("SUCCESS!");
                                        }
                                        break;
                                }
                                written = true;
                            }
                        }
                        ++x;
                    }
                    if (written) {
                        Console.WriteLine();
                        written = false;
                    }
                    var pendingBlob = CopyBlobProgress.FirstOrDefault(c => c.Value.Status == CopyStatus.Pending);
                    if (string.IsNullOrWhiteSpace(pendingBlob.Key))
                        checkForBlobCopyStatus = false;
                }
                if (string.IsNullOrWhiteSpace(continuationToken))
                    continueListObjects = false;
                Console.WriteLine("-");
                Console.WriteLine("");
            }
            Console.WriteLine("Process completed!");
            Console.Write("Press any key to terminate the program...");
            Console.ReadLine();
        }
    }
 
    public class CopyBlobProgress {
        public CopyStatus Status {
            get;
            set;
        }
 
        public Exception Error {
            get;
            set;
        }
    }
 
    public enum CopyStatus {
        NotStarted,
        Started,
        Aborted,
        Failed,
        Invalid,
        Pending,
        Success
    }
}