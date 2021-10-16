using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Sas;
using Microsoft.Azure.Cosmos;
using StorageConsoleApp.Models;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace StorageConsoleApp
{
    class Program
    {
        private static string _containerName = "democontainer";
        private static string _connectionString = "DefaultEndpointsProtocol=https;AccountName=icodestorage;AccountKey=5AQSLZuZsiDRy96gSNa5TfG5pUco0x/xMtVJGjuSW+KbmoWgM00Xf38uWcnysZjkyfTXD6PX0eMdBMKV0A0dig==;EndpointSuffix=core.windows.net";
        private static string _cosmosConString = "AccountEndpoint=https://cosmosaccount-siddharth.documents.azure.com:443/;AccountKey=nVJn3P9EOehzcOZuX2FJ2cfdUceubGZmcFZUtuwjoLkPbFpJhPAeFBnvSTAz8tGEWZsCjt3uLPGw1Se9U6W1XQ==;";
        static void Main(string[] args)
        {
            
            Console.ReadLine();
        }

 
        


        public static void CreateContainerCosmosDB()
        {
            CosmosClient cosmosClient = new CosmosClient("AccountEndpoint=https://cosmosaccountsiddharth.documents.azure.com:443/;AccountKey=yRqSC08Oncmt7LsZ9WSHP25Pntnfj1q26Srjau3uZxx8rTOvaqn3ZLdgX0tyYjOQx7fZfn7aU4hbEk9kIz6QvQ==;");
            cosmosClient.CreateDatabaseAsync("demodb").GetAwaiter().GetResult();
            Console.WriteLine("Database is created!");

            Database _cosmosDB = cosmosClient.GetDatabase("demodb");
            _cosmosDB.CreateContainerAsync("customer", "/customerid").GetAwaiter().GetResult();
            Console.WriteLine("Container is created!");
        }

        public static void AddItem()
        {
            CosmosClient cosmosClient = new CosmosClient("AccountEndpoint=https://cosmosaccountsiddharth.documents.azure.com:443/;AccountKey=yRqSC08Oncmt7LsZ9WSHP25Pntnfj1q26Srjau3uZxx8rTOvaqn3ZLdgX0tyYjOQx7fZfn7aU4hbEk9kIz6QvQ==;");
            Container container = cosmosClient.GetContainer("demodb", "customer");
            Customer c = new Customer();
            c.customerid = 1;
            c.CustomerName = "Sam";
            c.id = "1";
            container.CreateItemAsync<Customer>(c, new PartitionKey(c.customerid)).GetAwaiter().GetResult();
            Console.WriteLine("Item has been pushed!");
        }

        public static void AddItemsInBulk()
        {
            CosmosClient cosmosClient = new CosmosClient(_cosmosConString, new CosmosClientOptions() { AllowBulkExecution=true});
            Container container = cosmosClient.GetContainer("demodb", "customer");
            List<Customer> lst = new List<Customer>();
            lst.Add(new Customer() { id = "2", customerid = 2, CustomerName = "Mary" });
            lst.Add(new Customer() { id = "3", customerid = 3, CustomerName = "Simon" });
            lst.Add(new Customer() { id = "4", customerid = 4, CustomerName = "Kim" });
            lst.Add(new Customer() { id = "5", customerid = 5, CustomerName = "Joe" });

            List<Task> tasks = new List<Task>();
            foreach (var item in lst)
            {
                tasks.Add(container.CreateItemAsync<Customer>(item, new PartitionKey(item.customerid)));
            }

            Task.WhenAll(tasks).GetAwaiter().GetResult();
            Console.WriteLine("Items have been added in Bulk");

        }

        public static void ReadItems()
        {
            CosmosClient cosmosClient = new CosmosClient("AccountEndpoint=https://cosmosaccountsiddharth.documents.azure.com:443/;AccountKey=yRqSC08Oncmt7LsZ9WSHP25Pntnfj1q26Srjau3uZxx8rTOvaqn3ZLdgX0tyYjOQx7fZfn7aU4hbEk9kIz6QvQ==;", new CosmosClientOptions() { AllowBulkExecution = true });
            Container container = cosmosClient.GetContainer("demodb", "customer");
            string query = "select * from c";
            QueryDefinition queryDefinition = new QueryDefinition(query);
            FeedIterator<Customer> itr= container.GetItemQueryIterator<Customer>(queryDefinition);

            while(itr.HasMoreResults==true)
            {
                FeedResponse<Customer> cust= itr.ReadNextAsync().GetAwaiter().GetResult();
                foreach (var item in cust)
                {
                    Console.WriteLine("The ID is  \t"+item.customerid);
                    Console.WriteLine("The name is\t " + item.CustomerName);

                }
            }

        }



        public static void UpdateItem()
        {
            CosmosClient cosmosClient = new CosmosClient("AccountEndpoint=https://cosmosaccountsiddharth.documents.azure.com:443/;AccountKey=yRqSC08Oncmt7LsZ9WSHP25Pntnfj1q26Srjau3uZxx8rTOvaqn3ZLdgX0tyYjOQx7fZfn7aU4hbEk9kIz6QvQ==;", new CosmosClientOptions() { AllowBulkExecution = true });
            Container container = cosmosClient.GetContainer("demodb", "customer");
            string id = "1"; //this is the id I want to update
            int partitionKey = 1;
            ItemResponse<Customer> cust= container.ReadItemAsync<Customer>(id, new PartitionKey(partitionKey)).GetAwaiter().GetResult();
            Customer c = cust.Resource;
            c.CustomerName = "Siddharth";
            container.ReplaceItemAsync<Customer>(c, id, new PartitionKey(partitionKey)).GetAwaiter().GetResult();
            Console.WriteLine("Updation completed!");
        }





        public static void AddChangesUsingLease()
        {
            BlobServiceClient blobServiceClient = new BlobServiceClient(_connectionString);
            BlobContainerClient blobContainerClient = blobServiceClient.GetBlobContainerClient("democontainer");
            BlobClient blobClient = blobContainerClient.GetBlobClient("demo2.txt");

            MemoryStream memoryStream = new MemoryStream();
            blobClient.DownloadTo(memoryStream);
            memoryStream.Position = 0;

            StreamReader reader = new StreamReader(memoryStream);
            Console.WriteLine(reader.ReadToEnd());



            try
            {
                BlobLeaseClient blobLeaseClient = blobClient.GetBlobLeaseClient();
                BlobLease lease = blobLeaseClient.Acquire(TimeSpan.FromSeconds(30));


                StreamWriter writer = new StreamWriter(memoryStream);
                writer.Write("This is a change just to test!");
                writer.Flush();
              

                BlobUploadOptions uploadOptions = new BlobUploadOptions();
                BlobRequestConditions conditions = new BlobRequestConditions();
                conditions.LeaseId = lease.LeaseId;
                uploadOptions.Conditions = conditions;

                memoryStream.Position = 0;
                blobClient.Upload(memoryStream, uploadOptions);
                blobLeaseClient.Release();
                Console.WriteLine("File is updated!");
            }
            catch (Exception e)
            {

                Console.WriteLine("Some error occured!");
            }


        }

        public static void AddChanges()
        {
            BlobServiceClient blobServiceClient = new BlobServiceClient(_connectionString);
            BlobContainerClient blobContainerClient = blobServiceClient.GetBlobContainerClient("democontainer");
            BlobClient blobClient = blobContainerClient.GetBlobClient("demo2.txt");

            MemoryStream memoryStream = new MemoryStream();
            blobClient.DownloadTo(memoryStream);
            memoryStream.Position = 0;

            StreamReader reader = new StreamReader(memoryStream);
            Console.WriteLine(reader.ReadToEnd());

            try
            {
                StreamWriter writer = new StreamWriter(memoryStream);
                writer.Write("This is a change just to test!");
                writer.Flush();
                memoryStream.Position = 0;
                blobClient.Upload(memoryStream,true);
                Console.WriteLine("File is updated!");
            }
            catch (Exception e)
            {

                Console.WriteLine("Some error occured!");
            }
         

        }


        public static void SetMetaData()
        {
            BlobServiceClient blobServiceClient = new BlobServiceClient(_connectionString);
            BlobContainerClient blobContainerClient = blobServiceClient.GetBlobContainerClient("democontainer");
            BlobClient blobClient = blobContainerClient.GetBlobClient("sampleImage.jpg");

            BlobProperties property = blobClient.GetProperties();
            IDictionary<string, string> metadata = property.Metadata;
            try
            {
                metadata.Add("Version", "Original");
                blobClient.SetMetadata(metadata);
                Console.WriteLine("Meta data successfully added!");
            }
            catch (Exception)
            {
                Console.WriteLine("Some Error Occured!");
                
            }
        }

        public static void GetMetaData()
        {
            BlobServiceClient blobServiceClient = new BlobServiceClient(_connectionString);
            BlobContainerClient blobContainerClient = blobServiceClient.GetBlobContainerClient("democontainer");
            BlobClient blobClient = blobContainerClient.GetBlobClient("sampleImage.jpg");

            BlobProperties property = blobClient.GetProperties();

            IDictionary<string,string> metadata= property.Metadata;
            foreach (var item in metadata)
            {
                Console.WriteLine("Key is: "+item.Key+"\t Value is "+item.Value);
            }
        }

        public static void GetProperties()
        {
            BlobServiceClient blobServiceClient = new BlobServiceClient(_connectionString);
            BlobContainerClient blobContainerClient = blobServiceClient.GetBlobContainerClient("democontainer");
            BlobClient blobClient = blobContainerClient.GetBlobClient("sampleImage.jpg");


            BlobProperties property= blobClient.GetProperties();

            Console.WriteLine("The access level is "+property.AccessTier);


        }

      

        public static Uri GenerateStorageSAS()
        {
            BlobServiceClient b = new BlobServiceClient(_connectionString);

            AccountSasBuilder _builder = new AccountSasBuilder();
            _builder.ResourceTypes = AccountSasResourceTypes.All;
            _builder.Services = AccountSasServices.Blobs;
            _builder.SetPermissions(AccountSasPermissions.Read | AccountSasPermissions.List);
            _builder.ExpiresOn = DateTimeOffset.UtcNow.AddDays(1);
            return b.GenerateAccountSasUri(_builder);
        
        }

        public static Uri GenerateContainerSAS()
        {
            BlobServiceClient b = new BlobServiceClient(_connectionString);

            BlobContainerClient _containerClient = b.GetBlobContainerClient(_containerName);
            BlobSasBuilder _builder = new BlobSasBuilder();
            _builder.BlobContainerName = _containerName;
            _builder.Resource = "c";
            _builder.SetPermissions(BlobSasPermissions.Read | BlobSasPermissions.List);
            _builder.ExpiresOn = DateTimeOffset.UtcNow.AddDays(1);
            return _containerClient.GenerateSasUri(_builder);

        }
        public static Uri GenerateBlobSAS()
        {
            BlobServiceClient b = new BlobServiceClient(_connectionString);

            BlobContainerClient _containerClient = b.GetBlobContainerClient(_containerName);

            BlobClient _blobClient = _containerClient.GetBlobClient("sampleImage1.jpg");

            BlobSasBuilder _builder = new BlobSasBuilder();
            _builder.BlobContainerName = _containerName;
            _builder.BlobName = "sampleImage1.jpg";
            _builder.Resource = "b";
            _builder.SetPermissions(BlobSasPermissions.Read | BlobSasPermissions.List);
            _builder.ExpiresOn = DateTimeOffset.UtcNow.AddDays(1);
            return _blobClient.GenerateSasUri(_builder);

        }





    }
}
