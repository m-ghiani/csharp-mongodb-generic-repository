using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using GH.MongoDb.GenericRepository.Interfaces;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Linq;

namespace GH.MongoDb.GenericRepository
{
    public abstract class GenericRepositoryAsync<T, TKey> : IDisposable, IGenericRepositoryAsync<T, TKey>
        where T : IDocument<TKey>, new()
        where TKey : IEquatable<TKey>
    {
        protected GenericRepositoryAsync(IMongoDbConnector connector, string collectionName)
        {
            Connector = connector;

            CollectionName = collectionName;
            Collection = Connector.Db.GetCollection<T>(collectionName);
        }

        protected readonly string CollectionName;
        protected IMongoCollection<T> Collection { get; set; }
        public virtual void Dispose()
        {
            GC.SuppressFinalize(this);
        }
        public IMongoDbConnector Connector { get; }
        public virtual async Task<long> Count(Expression<Func<T, bool>> filter, CancellationToken token)
        {
            return await Collection.CountAsync(filter, cancellationToken: token);
        }
        public virtual async Task Add(T entity, CancellationToken token)
        {
            await Collection.InsertOneAsync(entity, new InsertOneOptions() { BypassDocumentValidation = false }, token);
        }
        public virtual async Task Add(List<T> entities, CancellationToken token)
        {
            await Collection.InsertManyAsync(entities, new InsertManyOptions() { BypassDocumentValidation = false }, token);
        }
        public virtual async Task Delete(TKey id, CancellationToken token)
        {
            await Collection.FindOneAndDeleteAsync(Builders<T>.Filter.Eq(e => e.Id, id), new FindOneAndDeleteOptions<T, T>(), token);
        }
        public virtual async Task DropCollection(CancellationToken token)
        {

            await Connector.Db.DropCollectionAsync(CollectionName, token);
        }
        public virtual async Task<bool> Exist(Expression<Func<T, bool>> filter, CancellationToken token)
        {

            return await IMongoCollectionExtensions.Find(Collection, filter).AnyAsync(token);
        }
        public virtual async Task<bool> ExistCollection(CancellationToken token)
        {

            return await IAsyncCursorExtensions.AnyAsync<BsonDocument>(Connector.Db
                    .ListCollections(new ListCollectionsOptions() { Filter = new BsonDocument("name", CollectionName) }), token);
        }
        public virtual async Task<IEnumerable<T>> Get(CancellationToken token)
        {
            return await IMongoCollectionExtensions.Find<T>(Collection, _ => true).ToListAsync(token);
        }
        public virtual async Task<IEnumerable<T>> Get(int offset, int limit, CancellationToken token)
        {
            return await IMongoCollectionExtensions.Find<T>(Collection, _ => true).Skip(offset).Limit(limit).ToListAsync(token);
        }
        public virtual async Task<IEnumerable<T>> Get(Expression<Func<T, bool>> filter, CancellationToken token)
        {
            return await IMongoCollectionExtensions.Find<T>(Collection, filter).ToListAsync(token);
        }
        public virtual async Task<IEnumerable<T>> Get(Expression<Func<T, bool>> filter, int offset, int limit, CancellationToken token)
        {
            return await IMongoCollectionExtensions.Find<T>(Collection, filter).Skip(offset).Limit(limit).ToListAsync(token);
        }
        public virtual async Task<T> Get(TKey id, CancellationToken token)
        {
            var filter = Builders<IDocument<TKey>>.Filter.Eq("_id", id);
            return await IMongoCollectionExtensions.Find<T>(Collection, e => filter.Inject()).SingleAsync(token);
        }
        public virtual async Task Update(T entity, CancellationToken token)
        {
            await IMongoCollectionExtensions.ReplaceOneAsync<T>(Collection, _ => Object.Equals(_.Id, entity.Id), entity, cancellationToken: token);
        }
        public virtual async Task Update(TKey id, T entity, CancellationToken token)
        {
            await IMongoCollectionExtensions.ReplaceOneAsync<T>(Collection, _ => Object.Equals(_.Id, id), entity, cancellationToken: token);
        }
        public virtual async Task Update(TKey id, string fieldName, object fieldValue, CancellationToken token)
        {
            var filter = Builders<T>.Filter.Eq(_ => Object.Equals(_.Id, id), true);
            var update = Builders<T>.Update.Set(fieldName, fieldValue);
            await Collection.UpdateOneAsync(filter, update, cancellationToken: token);
        }

    }
}