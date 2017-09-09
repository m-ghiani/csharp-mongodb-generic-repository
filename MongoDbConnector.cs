using System;
using System.Collections.Generic;
using System.Linq;
using GH.MongoDb.GenericRepository.Interfaces;
using MongoDB.Driver;
using MongoDB.Driver.GridFS;

namespace GH.MongoDb.GenericRepository
{
    public class MongoDbConnector : IMongoDbConnector
    {
        public MongoDbConnector(string dbServer, string dbName) => InitConnector(dbServer, dbName);
        public IMongoDatabase Db { get; private set; }

        public List<GridFSBucket> Buckets { get; set; }

        public bool HasBuckets => Buckets != null && Buckets.Any();

        public bool HasBucket(string bucketName) => Buckets != null && Buckets.Any(b => b.Options.BucketName == bucketName);

        public void Dispose() => GC.SuppressFinalize(this);

        private void InitConnector(string dbServer, string dbName)
        {
            MongoClient client;
            if (dbServer.Contains("/"))
            {
                client = new MongoClient(new MongoUrl(dbServer));
            }
            else
            {
                var settings = new MongoClientSettings() { Server = new MongoServerAddress(dbServer) };
                client = new MongoClient(settings);
            }
            Db = client.GetDatabase(dbName);
        }

        public GridFSBucket GetBucket(string bucketName)
        {
            if (HasBuckets && HasBucket(bucketName)) return Buckets.First(b => b.Options.BucketName == bucketName);
            if (!HasBuckets) Buckets = new List<GridFSBucket>();
            Buckets.Add(new GridFSBucket(Db, new GridFSBucketOptions()
            {
                BucketName = bucketName
            }));
            return Buckets.First(b => b.Options.BucketName == bucketName);
        }

    }
}