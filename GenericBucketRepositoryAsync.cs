using System;
using System.Threading;
using System.Threading.Tasks;
using GH.MongoDb.GenericRepository.Interfaces;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.GridFS;

namespace GH.MongoDb.GenericRepository
{
    public abstract class GenericBucketRepositoryAsync<T, TKey> : GenericRepositoryAsync<T, TKey>, IGenericRepositoryAsync<T, TKey> where T : IDocument<TKey>, new() where TKey : IEquatable<TKey>
    {
        private readonly GridFSBucket _bucket;
        protected GenericBucketRepositoryAsync(IMongoDbConnector connector, string collectionName) : base(connector, collectionName)
        {
            _bucket = Connector.GetBucket(collectionName);

        }
        public async Task<ObjectId> UploadFile(string fileName, byte[] source, GridFSUploadOptions options, CancellationToken token)
        {
            if (_bucket == null) return new ObjectId();
            return await _bucket.UploadFromBytesAsync(fileName, source, options, token);
        }
        public async Task<ObjectId> UploadFile(string fileName, byte[] source, CancellationToken token)
        {
            if (_bucket == null) return new ObjectId();
            return await _bucket.UploadFromBytesAsync(fileName, source, cancellationToken: token);
        }
        public async Task<byte[]> DownloadFile(ObjectId id, CancellationToken token)
        {
            if (_bucket == null) return null;
            return await _bucket.DownloadAsBytesAsync(id, cancellationToken: token);
        }
        public async Task<GridFSFileInfo> GetFileInfo(string fileName, CancellationToken token)
        {
            if (_bucket == null) return null;
            var filter = Builders<GridFSFileInfo>.Filter.Eq(x => x.Filename, fileName);
            var sort = Builders<GridFSFileInfo>.Sort.Descending(x => x.UploadDateTime);
            var options = new GridFSFindOptions
            {
                Limit = 1,
                Sort = sort
            };
            return await _bucket.Find(filter, options).FirstAsync<GridFSFileInfo>(token);
        }
        public async Task DeleteFile(ObjectId id, CancellationToken token)
        {
            if (_bucket == null) return;
            await _bucket.DeleteAsync(id, token);
        }
        public async Task RenameFile(ObjectId id, string newFileName, CancellationToken token)
        {
            if (_bucket == null) return;
            await _bucket.RenameAsync(id, newFileName, token);
        }
    }
}
