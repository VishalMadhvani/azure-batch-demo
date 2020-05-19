using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Auth;
using Microsoft.Azure.Batch.Common;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.Linq;

namespace BatchDotNetQuickstart
{
    public class Program
    {
        private const string BatchAccountName = "";
        private const string BatchAccountKey = "";
        private const string BatchAccountUrl = "";

        private const string StorageAccountName = "";
        private const string StorageAccountKey = "";

        private const string PoolId = "";
        private const int NumberOfInputFiles = 10;
        private const int NumberOfLinesInInputFiles = 1000;

        static void Main()
        {
            if (string.IsNullOrEmpty(BatchAccountName) ||
                string.IsNullOrEmpty(BatchAccountKey) ||
                string.IsNullOrEmpty(BatchAccountUrl) ||
                string.IsNullOrEmpty(StorageAccountName) ||
                string.IsNullOrEmpty(StorageAccountKey) ||
                string.IsNullOrEmpty(PoolId))
            {
                throw new InvalidOperationException("Missing Details");
            }
            Console.WriteLine("Starting");

            var executionId = Guid.NewGuid().ToString();
            var inputFileNames = Enumerable.Range(0, NumberOfInputFiles)
                .Select(i => Guid.NewGuid().ToString())
                .ToList();

            Console.WriteLine($"ExecutionId: {executionId}");
            Console.WriteLine($"Number of Files to Process: {inputFileNames.Count()}");

            using var batchClient = GetBatchClient();
            var blobContainer = GetBlobContainer(executionId);

            UploadInputFiles(blobContainer, inputFileNames);
            CreateJobAndTasks(batchClient, blobContainer, executionId, inputFileNames);
            WaitForTasksToComplete(batchClient, executionId);
            CleanUp(batchClient, blobContainer, executionId);

            Console.WriteLine("Finished");
        }

        private static void UploadInputFiles(CloudBlobContainer container, IEnumerable<string> inputFileNames)
        {
            foreach (var inputFileName in inputFileNames)
            {
                CloudBlockBlob blobData = container.GetBlockBlobReference($"input-files/{inputFileName}.txt");
                var content = string.Join(Environment.NewLine, Enumerable.Range(1, NumberOfLinesInInputFiles).Select(i => i.ToString()));
                blobData.UploadTextAsync(content);
                Console.WriteLine($"Uploaded File: {blobData.Name}");
            }
        }

        private static void CreateJobAndTasks(BatchClient batchClient, CloudBlobContainer container, string executionId, IEnumerable<string> inputFileNames)
        {
            string containerSas = container.GetSharedAccessSignature(new SharedAccessBlobPolicy()
            {
                Permissions = SharedAccessBlobPermissions.Read | SharedAccessBlobPermissions.Write,
                SharedAccessExpiryTime = DateTimeOffset.UtcNow.AddDays(1)
            });

            var job = batchClient.JobOperations.CreateJob(executionId, new PoolInformation { PoolId = PoolId });
            job.OnAllTasksComplete = OnAllTasksComplete.TerminateJob;
            job.Commit();

            List<CloudTask> tasks = new List<CloudTask>();

            foreach (var inputFileName in inputFileNames)
            {
                string taskCommandLine = $"cmd /c %AZ_BATCH_APP_PACKAGE_azurebatchdemo-task%\\AzureBatchDemo.Task.exe {inputFileName}.txt";

                var task = new CloudTask(inputFileName, taskCommandLine)
                {
                    EnvironmentSettings = new List<EnvironmentSetting>() { new EnvironmentSetting("AZ_BATCH_DEMO_BLOB_CONTAINER_NAME", executionId) },
                    ResourceFiles = new List<ResourceFile>() {
                        ResourceFile.FromUrl($"{container.Uri.AbsoluteUri}/input-files/{inputFileName}.txt{containerSas}", $"{inputFileName}.txt") },
                    OutputFiles = new List<OutputFile>() {
                        new OutputFile(
                            "output\\*.txt",
                            new OutputFileDestination(
                                new OutputFileBlobContainerDestination(
                                    container.Uri.AbsoluteUri + containerSas,
                                    $"output-files/{inputFileName}")),
                            new OutputFileUploadOptions(OutputFileUploadCondition.TaskCompletion)) },
                    Constraints = new TaskConstraints(maxTaskRetryCount: -1)
                };
                tasks.Add(task);
            }

            batchClient.JobOperations.AddTask(executionId, tasks);

            Console.WriteLine("Job and Tasks created.");
        }

        private static void WaitForTasksToComplete(BatchClient batchClient, string executionId)
        {
            Console.WriteLine("Waiting for Tasks to complete...");

            var tasks = batchClient.JobOperations.ListTasks(executionId);
            batchClient.Utilities.CreateTaskStateMonitor().WaitAll(tasks, TaskState.Completed, TimeSpan.FromMinutes(30));

            Console.WriteLine("Tasks Completed.");
        }

        private static void CleanUp(BatchClient batchClient, CloudBlobContainer container, string executionId)
        {
            Console.WriteLine("Delete Batch Job and Blob Container? [Y/n]");
            if (Console.ReadLine() != "n")
            {
                Console.WriteLine("Cleaning up resources...");
                batchClient.JobOperations.GetJob(executionId).Delete();
                container.Delete();
            }
        }

        private static BatchClient GetBatchClient()
        {
            var batchCredentials = new BatchSharedKeyCredentials(BatchAccountUrl, BatchAccountName, BatchAccountKey);
            return BatchClient.Open(batchCredentials);
        }

        private static CloudBlobContainer GetBlobContainer(string executionId)
        {
            string storageConnectionString =
                $"DefaultEndpointsProtocol=https;AccountName={StorageAccountName};AccountKey={StorageAccountKey}";

            var storageAccount = CloudStorageAccount.Parse(storageConnectionString);
            var client = storageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(executionId);
            container.CreateIfNotExists();
            return container;
        }
    }
}