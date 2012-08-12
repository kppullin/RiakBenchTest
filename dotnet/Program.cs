using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Linq;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using CorrugatedIron;
using CorrugatedIron.Models;

namespace RiakBench
{
    static class Program
    {
		static void Main()
        {
            var cluster = RiakCluster.FromConfig("riakConfig");

            var bucket = ConfigurationManager.AppSettings["bucket"];
			var maxThreads = int.Parse(ConfigurationManager.AppSettings["maxThreads"]);
            var maxObjSizeKB = int.Parse(ConfigurationManager.AppSettings["maxObjSizeKB"]);
            var minObjSizeKB = int.Parse(ConfigurationManager.AppSettings["minObjSizeKB"]);
            var itemsToStore = int.Parse(ConfigurationManager.AppSettings["itemsToStore"]);

            if (minObjSizeKB > maxObjSizeKB)
                throw new Exception("minObjSizeKB must be less than or equal to maxObjSizeKB");

			DeleteExistingKeys(cluster, bucket);
        
			// collection of all timing results
			var individualTimings = new ConcurrentBag<Stopwatch>();
			var stopwatch = new Stopwatch();
			var storedObjects = StoreObjects(cluster, bucket, maxThreads, maxObjSizeKB, minObjSizeKB, itemsToStore, individualTimings, stopwatch);

			PrintResults(itemsToStore, individualTimings, stopwatch, storedObjects.Sum(x => x.Size));

            VerifyObjects(cluster, storedObjects);

            Console.WriteLine("\r\nPress any key to exit");
            Console.ReadKey(true);
        }

        public class StoredObject
        {
            public RiakObject Value { get; set; }
            public int Size { get; set; }
            public string Hash { get; set; }
        }

		private static ConcurrentBag<StoredObject> StoreObjects(IRiakEndPoint cluster, string bucket, int maxThreads, int maxObjSizeKB, int minObjSizeKB, int itemsToStore, ConcurrentBag<Stopwatch> individualTimings, Stopwatch stopwatch)
		{
			// Create pool of Riak Clients and Random objects
			var riakResourcePool = new ConcurrentBag<RiakBenchmarkResource>();
            for (int i = 0; i < maxThreads; ++i)
				riakResourcePool.Add(new RiakBenchmarkResource { Client = cluster.CreateClient(), Random = new Random() });

			var size = maxObjSizeKB * 1024 * 2; // multiply by two to make random buffer copy math easier below
			var randomBytes = GetRandomByteArray(size);

            var storedObjects = new ConcurrentBag<StoredObject>();

			stopwatch.Start();

			int count = 0;
			int totalSize = 0;
			try
			{
				Console.WriteLine("Storing {0} objects in Riak", itemsToStore);
				Parallel.For(0, itemsToStore, new ParallelOptions { MaxDegreeOfParallelism = maxThreads }, i =>
				{
					var iterationStopwatch = Stopwatch.StartNew();

					RiakBenchmarkResource riakResource;
					riakResourcePool.TryTake(out riakResource);
					try
					{
						var rand = riakResource.Random;
                        var item = new byte[rand.Next(minObjSizeKB * 1024, maxObjSizeKB * 1024) + 1];
						Array.Copy(randomBytes, rand.Next(0, maxObjSizeKB * 1024), item, 0, item.Length);

						var obj = new RiakObject(bucket, Guid.NewGuid().ToString(), item, "application/binary", CorrugatedIron.Util.RiakConstants.CharSets.Utf8);

                        var result = riakResource.Client.Put(obj, new RiakPutOptions { ReturnBody = false, W = 2 });
						if (!result.IsSuccess)
							System.Diagnostics.Debugger.Break();

						int countSnapshot = Interlocked.Increment(ref count);
						Interlocked.Add(ref totalSize, item.Length);

						if (countSnapshot % Math.Max(itemsToStore / 10, 1) == 0)
							Console.WriteLine("Processed {0} of {1}. Elapsed: {2}", count, itemsToStore, stopwatch.Elapsed);

                        storedObjects.Add(new StoredObject { Value = result.Value, Size = item.Length, Hash = GetHash(item) });
					}
					finally
					{
						riakResourcePool.Add(riakResource);
						iterationStopwatch.Stop();
						individualTimings.Add(iterationStopwatch);
					}
				});
			}
			catch (Exception ex)
			{
				Console.WriteLine(ex.ToString());
			}

			stopwatch.Stop();
			return storedObjects;
		}

        private static void VerifyObjects(IRiakEndPoint cluster, ConcurrentBag<StoredObject> storedObjects)
        {
            var client = cluster.CreateClient();
            foreach (var storedObj in storedObjects)
            {
                var result = client.Get(new RiakObjectId { Bucket = storedObj.Value.Bucket, Key = storedObj.Value.Key });
                var hash = GetHash(result.Value.Value);
                if (hash != storedObj.Hash)
                    throw new Exception("Put & Get checksums do not match");
            }
        }

		private static void DeleteExistingKeys(IRiakEndPoint cluster, string bucket)
		{
			// Delete all keys in bucket... maybe should just delete the bucket itself
            var keys = cluster.CreateClient().StreamListKeys(bucket).Value.ToList();
            
			Console.WriteLine("Cleaning up {0} existing items", keys.Count);
			foreach (var key in keys)
			{
				var res = cluster.CreateClient().Delete(new RiakObjectId { Bucket = bucket, Key = key });
			}
		}

		private static void PrintResults(int itemsToStore, ConcurrentBag<Stopwatch> individualTimings, Stopwatch stopwatch, int totalSize)
		{
			Console.WriteLine("Elapsed: " + stopwatch.Elapsed);
			Console.WriteLine("Bytes: " + totalSize / 1024 / 1024 + "MB");

			// redis style
			int runningTotal = 0;
			foreach (var timingGroup in individualTimings
				.GroupBy(x => x.ElapsedMilliseconds <= 10 ? x.ElapsedMilliseconds : ((int)(x.ElapsedMilliseconds / 10)) * 10 + 10)    // normalize to 10 second intervals, rounded up
				.OrderBy(x => x.Key))
			{
				runningTotal += timingGroup.Count();
				Console.WriteLine("{0}% <= {1} milliseconds", runningTotal / (double)itemsToStore * 100, timingGroup.Key);
			}
		}

        private static byte[] GetRandomByteArray(int size)
        {
            var randomBytes = new byte[size];
            var rand = new Random();

            for (int i = 0; i < size; ++i)
            {
                randomBytes[i] = (byte)rand.Next((int)'A', (int)'z' + 1);
            }
            return randomBytes;
        }

        private static string GetHash(byte[] bytes)
        {
            using (SHA1CryptoServiceProvider sha1 = new SHA1CryptoServiceProvider())
            {
                var hashBytes = sha1.ComputeHash(bytes);
                string result = "";
                foreach (var hashByte in hashBytes)
                    result += string.Format("{0:X2}", hashByte);
                return result;
            }
        }

        public class RiakBenchmarkResource
        {
            public IRiakClient Client { get; set; }
            public Random Random { get; set; }
        }
    }
}
