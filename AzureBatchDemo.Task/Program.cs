using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace AzureBatchDemo.TaskApplication
{
    class Program
    {
        private const string StorageAccountName = "";
        private const string StorageAccountKey = "";

        private const string DefaultInputFilePath = "sample-input.txt";
        private const string DefaultBlobContainerName = "test";

        static void Main(string[] args)
        {
            if (string.IsNullOrEmpty(StorageAccountName) ||
                string.IsNullOrEmpty(StorageAccountKey))
            {
                throw new InvalidOperationException("Missing Details");
            }

            Console.WriteLine("Starting");

            var inputFile = new FileInfo(args.FirstOrDefault() ?? DefaultInputFilePath);
            var inputFileName = Path.GetFileNameWithoutExtension(inputFile.Name);

            Console.WriteLine($"Input File: {inputFile.Name}");

            var allChunks = GetChunksFromInputFile(inputFile);
            var processedChunkIndexes = GetProcessedChunkIndexes(inputFileName);
            var remainingChunks = GetRemainingChunksToBeProcessed(allChunks, processedChunkIndexes);

            Console.WriteLine($"Total Number of Chunks: {allChunks.Count()}");
            Console.WriteLine($"Number of Processed Chunks: {processedChunkIndexes.Count()}");
            Console.WriteLine($"Remaining Chunks to Process: {remainingChunks.Count()}");

            Directory.CreateDirectory("output");

            var tasks = remainingChunks
                .Select((kvp) => Task.Run(() => ProcessChunk(kvp.Key, kvp.Value)));

            Task.WaitAll(tasks.ToArray());

            Console.Write("All remaining work complete.");
        }

        private static void ProcessChunk(int chunkId, string content)
        {
            Console.WriteLine($"Starting to process chunk: {chunkId}");
            Thread.Sleep(1000);
            File.WriteAllText($"output\\{chunkId}.txt", content);
            Console.WriteLine($"Finished processing chunk: {chunkId}");
        }

        private static IEnumerable<string> GetChunksFromInputFile(FileInfo inputFile)
        {
            return File.ReadAllText(inputFile.Name).Split(Environment.NewLine);
        }

        private static IEnumerable<int> GetProcessedChunkIndexes(string inputFileName)
        {
            var container = GetBlobContainer();
            var blobs = container.ListBlobsSegmentedAsync($"output-files/{inputFileName}/", null).Result;
            var processedChunkIndexes = blobs.Results
                .OfType<CloudBlockBlob>()
                .Select(blob => blob.Name.Substring($"output-files/{inputFileName}/".Length))
                .Select(name => name.Remove(name.Length - 4))
                .Select(name => Convert.ToInt32(name));

            return processedChunkIndexes;
        }

        private static IDictionary<int, string> GetRemainingChunksToBeProcessed(IEnumerable<string> allChunks, IEnumerable<int> processedChunkIndexes)
        {
            var remainingChunksToBeProcessed = new Dictionary<int, string>();
            for (int chunkIndex = 0; chunkIndex < allChunks.Count(); chunkIndex++)
            {
                if (!processedChunkIndexes.Contains(chunkIndex))
                {
                    remainingChunksToBeProcessed.Add(chunkIndex, allChunks.ElementAt(chunkIndex));
                }
            }

            return remainingChunksToBeProcessed;
        }

        private static CloudBlobContainer GetBlobContainer()
        {
            var containerName = Environment.GetEnvironmentVariable("AZ_BATCH_DEMO_BLOB_CONTAINER_NAME") ?? DefaultBlobContainerName;

            string storageConnectionString =
                $"DefaultEndpointsProtocol=https;AccountName={StorageAccountName};AccountKey={StorageAccountKey}";

            var storageAccount = CloudStorageAccount.Parse(storageConnectionString);
            var blobClient = storageAccount.CreateCloudBlobClient();
            var blobContainer = blobClient.GetContainerReference(containerName);
            blobContainer.CreateIfNotExists();

            return blobContainer;
        }
    }
}
