using GH.MongoDb.GenericRepository.Interfaces;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.GridFS;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Authentication;
using System.Threading.Tasks;
using MongoDB.Bson.IO;
using MongoDB.Bson.Serialization;

namespace GH.MongoDb.GenericRepository
{
    /// <summary>
    /// MongoDb Connector. Use this class to connect with any mongodb database
    /// </summary>
    public class MongoDbConnector : IMongoDbConnector
    {
        /// <summary>
        /// Use mongodb connection string to connect with database
        /// <see cref="http://docs.mongodb.com/manual/reference/connection-string/"/>
        /// </summary>
        /// <param name="connectionString">the connection string for the database</param>
        public MongoDbConnector(string connectionString) => IsConnected = InitConnector(connectionString, string.Empty, string.Empty, string.Empty);
 
        /// <summary>
        /// Use mongodb connection string to connect with database
        /// <see cref="http://docs.mongodb.com/manual/reference/connection-string/"/>
        /// </summary>
        /// <param name="connectionString">the connection string for the database</param>
        /// <param name="protocols">ssl protocols</param>
        public MongoDbConnector(string connectionString, SslProtocols protocols) => IsConnected = InitConnector(connectionString, string.Empty, string.Empty, string.Empty, protocols);

        /// <summary>
        /// Use server name and dbname to connect with database eg: dbServer = "localhost", dbName = "databasename".
        /// Use this contructor for databases whit no credentials
        /// </summary>
        /// <param name="dbServer">server name</param>
        /// <param name="dbName">database name</param>
        public MongoDbConnector(string dbServer, string dbName) => IsConnected = InitConnector(dbServer, dbName, string.Empty, string.Empty);
        
        /// <summary>
        /// Use server name and dbname to connect with database eg: dbServer = "localhost", dbName = "databasename".
        /// Use this contructor for databases whit credentials
        /// </summary>
        /// <param name="dbServer">server name</param>
        /// <param name="dbName">database name</param>
        /// <param name="username">username</param>
        /// <param name="password">password</param>
        public MongoDbConnector(string dbServer, string dbName, string username, string password) => IsConnected = InitConnector(dbServer, dbName, username, password);
        
        /// <summary>
        /// Use server name and dbname to connect with database eg: dbServer = "localhost", dbName = "databasename".
        /// Use this contructor for databases whit credentials
        /// </summary>
        /// <param name="dbServer">server name</param>
        /// <param name="dbName">database name</param>
        /// <param name="username">username</param>
        /// <param name="password">password</param>
        /// <param name="protocols">ssl protocols</param>
        public MongoDbConnector(string dbServer, string dbName, string username, string password,SslProtocols protocols) => IsConnected = InitConnector(dbServer, dbName, username, password,protocols);
        
        /// <summary>
        /// Represents the database object
        /// </summary>
        public IMongoDatabase Db { get; private set; }
        
        /// <summary>
        /// Return connection status after the class was instantiated.
        /// If return false, check HasError property to know why connection failed
        /// </summary>
        public bool IsConnected { get; }
        
        /// <summary>
        /// It returns the connection failure reason 
        /// </summary>
        public string HasError { get; private set; }
        
        /// <summary>
        /// It returns GridFs bucket objects list
        /// </summary>
        public List<GridFSBucket> Buckets { get; set; }
        
        /// <summary>
        /// It checks if database contains GridFs buckets
        /// </summary>
        public bool HasBuckets => Buckets != null && Buckets.Any();
        
        /// <summary>
        /// It checks if database contains a specific GridFs bucket
        /// </summary>
        /// <param name="bucketName">bucket to check</param>
        /// <returns></returns>
        public bool HasBucket(string bucketName) => Buckets != null && Buckets.Any(b => b.Options.BucketName == bucketName);

        public void Dispose() => GC.SuppressFinalize(this);
        
        /// <summary>
        /// Get a specific bucket, if it does not exist it is created
        /// <see cref="http://api.mongodb.com/csharp/current/html/T_MongoDB_Driver_GridFS_MongoGridFS.htm"/>
        /// </summary>
        /// <param name="bucketName"></param>
        /// <returns></returns>
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
        
        /// <summary>
        /// Export a specific collection to json file
        /// </summary>
        /// <param name="collectionName">name of the collection</param>
        /// <param name="outputFileName">file name</param>
        /// <returns></returns>
        public async Task ExportCollection(string collectionName, string outputFileName)
        {
            IMongoCollection<BsonDocument> collection = Db.GetCollection<BsonDocument>(collectionName);
            using (var streamWriter = new StreamWriter(outputFileName))
            {
                await collection.Find(new BsonDocument())
                    .ForEachAsync(async (document) =>
                    {
                        using (var stringWriter = new StringWriter())
                        using (var jsonWriter = new JsonWriter(stringWriter))
                        {
                            var context = BsonSerializationContext.CreateRoot(jsonWriter);
                            collection.DocumentSerializer.Serialize(context, document);
                            var line = stringWriter.ToString();
                            await streamWriter.WriteLineAsync(line);
                        }
                    });
            }
        }

        private bool InitConnector(string dbServer, string dbName, string username, string password, SslProtocols protocols=SslProtocols.None)
        {
            MongoClientSettings settings;
            if (dbServer.Contains("mongodb://"))
            {
                settings = MongoClientSettings.FromUrl(new MongoUrl(dbServer));
            }
            else
            {
                settings = new MongoClientSettings() { Server = new MongoServerAddress(dbServer) };
                if (!string.IsNullOrEmpty(username) && !string.IsNullOrEmpty(password))
                {
                    var credential = MongoCredential.CreateCredential(dbName, username, password);
                    settings.Credentials = new List<MongoCredential>() { credential };
                }
            }
            if (protocols != SslProtocols.None)
            {
                settings.SslSettings = new SslSettings() { EnabledSslProtocols = protocols };
            }

            var client = new MongoClient(settings);
            try
            {
                Db = client.GetDatabase(dbName);
                Db.RunCommandAsync((Command<BsonDocument>)"{ping:1}").Wait();
                return true;
            }
            catch (Exception ex)
            {
                HasError = $"Connection error: {ex.Message}";
                return false;
            }
        }
    }
}