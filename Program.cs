using System;
using System.Collections.Specialized;
using System.IO;
using System.Text;
using System.Threading;
using Digimarc.Shared.Azure;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.StorageClient;

namespace BlobLeasing
{
	class Program
	{
		private const string file = "test/folder/Daily.txt";
		private const string connection = "DefaultEndpointsProtocol=https;AccountName=debugmadrasportal;AccountKey=hLcznt8xNkInV4p5Z3jAKz66rxd7Lj6u/9iMJbsl7YoMcym6a+jWUdb2PrLHATG79AaDckIG/9DpHZdzWDli7g==";
		static BlobAccess blob;

		static void Main()
		{
			TestLeasing();
			//TestBlobManipulation();
		}

		private static void TestBlobManipulation()
		{
			blob = new BlobAccess(connection);
			byte[] byteArray = Encoding.ASCII.GetBytes("Data for the file");
			var stream = new MemoryStream(byteArray);

			var metadata = new NameValueCollection();
			metadata.Add("Created", DateTime.UtcNow.ToString());
			metadata.Add("Type", "Any old type");

			const string myFile = "private/brett/testfile.txt";
			blob.Create(myFile, stream, "text/plain", metadata);
			BlobAttributes attr = blob.GetAttributes(myFile);
		}

		private static void TestLeasing()
		{
			blob = new BlobAccess(connection);
			//string id1 = Acquire();
			//string id2 = Acquire();
			//Console.WriteLine("Released: " + blob.LeaseRelease(file, id1));
			//id2 = Acquire();
			//Console.WriteLine("Released: " + blob.LeaseRelease(file, id2));
			//id1 = Acquire();
			//Console.WriteLine("Released: " + blob.LeaseRelease(file, id1));

			var store = CloudStorageAccount.Parse(connection);
			var client = new CloudBlobClient(store.BlobEndpoint, store.Credentials);
			CloudBlobContainer container = client.GetContainerReference("test");
			CloudBlob cb = container.GetBlobReference("folder/Daily.txt");

			using (var lease = new AutoRenewLease(cb))
			{
				//lease.RenewIntervalSec = 5;
				if (lease.HasLease)
				{
					Console.WriteLine("I have the leaseId: " + lease.LeaseId + " at: " + DateTime.Now.ToLongTimeString());
					//string id = Acquire();
					//cb.ReleaseLease(lease.LeaseId);
					//id = Acquire();
					//cb.ReleaseLease(id);
					Thread.Sleep(62 * 1000);
					Console.WriteLine("I have the leaseId: " + lease.LeaseId + " at: " + DateTime.Now.ToLongTimeString());
					Thread.Sleep(30 * 1000);
					Console.WriteLine("I have the leaseId: " + lease.LeaseId + " at: " + DateTime.Now.ToLongTimeString());
					Thread.Sleep(10 * 1000);
					Console.WriteLine("I have the leaseId: " + lease.LeaseId + " at: " + DateTime.Now.ToLongTimeString());
					Console.WriteLine("Letting go of lease at: " + DateTime.Now.ToLongTimeString());
				}
				else
				{
					Console.WriteLine("Could not get lease at: " + DateTime.Now.ToLongTimeString());
				}
			}
			Console.ReadKey();
		}

		static string Acquire()
		{
			try
			{
				string id = blob.LeaseAcquire(file);
				Console.WriteLine("Lease Id: " + id);
				return id;
			}
			catch (Exception e)
			{
				Console.WriteLine(e.Message);
				return "";
			}
		}
	}
}
