using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using GH.MongoDb.GenericRepository.Interfaces;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.GeoJsonObjectModel;
using MongoDB.Driver.GridFS;

namespace GH.MongoDb.GenericRepository
{


    public static class GenericRepositoryAsyncExtension

    {
        public static async Task<ObjectId> UploadFile<T,TKey>(this GenericRepositoryAsync<T,TKey> repo, string fileName, byte[] source, GridFSUploadOptions options, CancellationToken token=default(CancellationToken)) where T : IDocument<TKey>, new() where TKey : IEquatable<TKey>
        {

            var bucket = repo.Connector.GetBucket(repo.CollectionName);
            if (bucket == null) return new ObjectId();
            return await bucket.UploadFromBytesAsync(fileName, source, options, token);
        }

        public static async Task<ObjectId> UploadFile<T, TKey>(this GenericRepositoryAsync<T, TKey> repo, string fileName, byte[] source, CancellationToken token = default(CancellationToken)) where T : IDocument<TKey>, new() where TKey : IEquatable<TKey>
        {
            var bucket = repo.Connector.GetBucket(repo.CollectionName);
            if (bucket == null) return new ObjectId();
            return await bucket.UploadFromBytesAsync(fileName, source, cancellationToken: token);
        }
        public static async Task<byte[]> DownloadFile<T, TKey>(this GenericRepositoryAsync<T, TKey> repo, ObjectId id, CancellationToken token = default(CancellationToken)) where T : IDocument<TKey>, new() where TKey : IEquatable<TKey>
        {
            var bucket = repo.Connector.GetBucket(repo.CollectionName);
            if (bucket == null) return null;
            return await bucket.DownloadAsBytesAsync(id, cancellationToken: token);
        }
        public static async Task<GridFSFileInfo> GetFileInfo<T, TKey>(this GenericRepositoryAsync<T, TKey> repo, string fileName, CancellationToken token = default(CancellationToken)) where T : IDocument<TKey>, new() where TKey : IEquatable<TKey>
        {
            var bucket = repo.Connector.GetBucket(repo.CollectionName);
            if (bucket == null) return null;
            var filter = Builders<GridFSFileInfo>.Filter.Eq(x => x.Filename, fileName);
            var sort = Builders<GridFSFileInfo>.Sort.Descending(x => x.UploadDateTime);
            var options = new GridFSFindOptions
            {
                Limit = 1,
                Sort = sort
            };
            return await bucket.Find(filter, options).FirstAsync(token);
        }
        public static async Task DeleteFile<T, TKey>(this GenericRepositoryAsync<T, TKey> repo, ObjectId id, CancellationToken token = default(CancellationToken)) where T : IDocument<TKey>, new() where TKey : IEquatable<TKey>
        {
            var bucket = repo.Connector.GetBucket(repo.CollectionName);
            if (bucket == null) return;
            await bucket.DeleteAsync(id, token);
        }
        public static async Task RenameFile<T, TKey>(this GenericRepositoryAsync<T, TKey> repo, ObjectId id, string newFileName, CancellationToken token = default(CancellationToken)) where T : IDocument<TKey>, new() where TKey : IEquatable<TKey>
        {
            var bucket = repo.Connector.GetBucket(repo.CollectionName);
            if (bucket == null) return;
            await bucket.RenameAsync(id, newFileName, token);
        }

        public static async Task<IEnumerable<T>> Get<T, TKey>(this GenericRepositoryAsync<T, TKey> repo, double latitude, double longitude, double distance, Expression<Func<T, bool>> filter, int? skip, int? limit, CancellationToken token = default(CancellationToken)) where T : IDocument<TKey>, ILocationDocument, new() where TKey : IEquatable<TKey>
        {
            if (!repo.CollectionExist) return new List<T>();
            FilterDefinition<T> query = null;
            if (distance > 100 && distance < 50000)
            {
                var gp = new GeoJsonPoint<GeoJson2DGeographicCoordinates>(new GeoJson2DGeographicCoordinates(latitude, longitude));
                query = Builders<T>.Filter.Near(s => s.Location, gp, distance);
            }
            query = query != null ? query & filter : filter;

            return await repo.Collection.Find(query).Skip(skip).Limit(limit).ToListAsync(token);

        }

        public static async Task<long> Count<T, TKey>(this GenericRepositoryAsync<T, TKey> repo, double latitude, double longitude, double distance, Expression<Func<T, bool>> filter, CancellationToken token = default(CancellationToken)) where T : IDocument<TKey>, ILocationDocument, new() where TKey : IEquatable<TKey>
        {
            if (!repo.CollectionExist) return 0;
            FilterDefinition<T> query = null;
            if (distance > 100 && distance < 50000)
            {
                var gp = new GeoJsonPoint<GeoJson2DGeographicCoordinates>(new GeoJson2DGeographicCoordinates(latitude, longitude));
                query = Builders<T>.Filter.Near(s => s.Location, gp, distance);
            }
            query = query != null ? query & filter : filter;

            return await repo.Collection.CountAsync(query, cancellationToken: token);

        }


    }
}
