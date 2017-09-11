using GH.MongoDb.GenericRepository.Interfaces;
using GH.MongoDb.GenericRepository.PagingOptions;
using GH.MongoDb.GenericRepository.SortingOptions;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

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
        public virtual void Dispose() => GC.SuppressFinalize(this);
        public IMongoDbConnector Connector { get; }
        public virtual async Task<long> Count(Expression<Func<T, bool>> filter, CancellationToken token) => await Collection.CountAsync(filter, cancellationToken: token);
        public virtual async Task Add(T entity, CancellationToken token) => await Collection.InsertOneAsync(entity, new InsertOneOptions() { BypassDocumentValidation = false }, token);
        public virtual async Task Add(List<T> entities, CancellationToken token) => await Collection.InsertManyAsync(entities, new InsertManyOptions() { BypassDocumentValidation = false }, token);
        public virtual async Task Delete(TKey id, CancellationToken token) => await Collection.FindOneAndDeleteAsync(Builders<T>.Filter.Eq(e => e.Id, id), new FindOneAndDeleteOptions<T, T>(), token);
        public virtual async Task DropCollection(CancellationToken token) => await Connector.Db.DropCollectionAsync(CollectionName, token);
        public virtual async Task<bool> Exist(Expression<Func<T, bool>> filter, CancellationToken token) => await Collection.Find(filter).AnyAsync(token);
        public virtual async Task<bool> ExistCollection(CancellationToken token) => await Connector.Db.ListCollections(new ListCollectionsOptions() { Filter = new BsonDocument("name", CollectionName) }).AnyAsync(token);
        public virtual async Task<IEnumerable<T>> Get(CancellationToken token) => (IEnumerable<T>)await GetCollection(null, null, null, null, token);
        public virtual async Task<IEnumerable<T>> Get(int? limit, CancellationToken token) => (IEnumerable<T>)await GetCollection(null, new PagingSettings(null, limit), null, null, token);
        public virtual async Task<IEnumerable<T>> Get(Expression<Func<T, bool>> filter, CancellationToken token) => (IEnumerable<T>) await GetCollection(filter,null,null,null,token);
        public virtual async Task<IEnumerable<T>> Get(Expression<Func<T, bool>> filter, int? limit, CancellationToken token) => (IEnumerable<T>) await GetCollection(filter, new PagingSettings(null,limit), null, null, token);
        public virtual async Task<IEnumerable<T>> Get(Expression<Func<T, bool>> filter, int? skip, int? limit, CancellationToken token) => (IEnumerable<T>) await GetCollection(filter, new PagingSettings(skip, limit), null, null, token);
        public virtual async Task<IEnumerable<T>> Get(Expression<Func<T, bool>> filter, PagingSettings settings, CancellationToken token) => (IEnumerable<T>)await GetCollection(filter, settings, null, null, token);
        public virtual async Task<IEnumerable<T>> Get(Expression<Func<T, bool>> filter, PagingSettings settings, IEnumerable<SortingField> sortings, CancellationToken token) => (IEnumerable<T>)await GetCollection(filter, settings,sortings, null, token);
        public virtual async Task<IEnumerable<T>> Get(Expression<Func<T, bool>> filter, PagingSettings settings, Expression<Func<T, object>> projection, CancellationToken token) => (IEnumerable<T>)await GetCollection(filter, settings, null, projection, token);
        public virtual async Task<IEnumerable<T>> Get(Expression<Func<T, bool>> filter, PagingSettings settings, IEnumerable<SortingField> sortings, Expression<Func<T,object>> projection, CancellationToken token) => (IEnumerable<T>)await GetCollection(filter, settings, sortings, projection, token);
        public virtual async Task<IEnumerable<T>> Get(Expression<Func<T, bool>> filter, Expression<Func<T, object>> projection, CancellationToken token) => (IEnumerable<T>)await GetCollection(filter, null, null, projection, token);
        public virtual async Task<IEnumerable<T>> Get(Expression<Func<T, bool>> filter, IEnumerable<SortingField> sortings, CancellationToken token) => (IEnumerable<T>)await GetCollection(filter, null, sortings, null, token);
        public virtual async Task<IEnumerable<T>> Get(Expression<Func<T, bool>> filter, IEnumerable<SortingField> sortings, Expression<Func<T, object>> projection, CancellationToken token) => (IEnumerable<T>)await GetCollection(filter, null, sortings, projection, token);
        public virtual async Task<IEnumerable<T>> Get(int? skip, int? limit, CancellationToken token) => (IEnumerable<T>)await GetCollection(null, new PagingSettings(skip, limit), null, null, token);
        public virtual async Task<IEnumerable<T>> Get(PagingSettings settings, CancellationToken token) => (IEnumerable<T>)await GetCollection(null, settings, null, null, token);
        public virtual async Task<IEnumerable<T>> Get(PagingSettings settings, IEnumerable<SortingField> sortings, CancellationToken token) => (IEnumerable<T>)await GetCollection(null, settings, sortings, null, token);
        public virtual async Task<IEnumerable<T>> Get(PagingSettings settings, Expression<Func<T, object>> projection, CancellationToken token) => (IEnumerable<T>)await GetCollection(null, settings, null, projection, token);
        public virtual async Task<IEnumerable<T>> Get(PagingSettings settings, IEnumerable<SortingField> sortings, Expression<Func<T, object>> projection, CancellationToken token) => (IEnumerable<T>)await GetCollection(null, settings, sortings, projection, token);
        public virtual async Task<IEnumerable<T>> Get(IEnumerable<SortingField> sortings, CancellationToken token) => (IEnumerable<T>)await GetCollection(null, null, sortings, null, token);
        public virtual async Task<IEnumerable<T>> Get(Expression<Func<T, object>> projection, CancellationToken token) => (IEnumerable<T>)await GetCollection(null, null, null, projection, token);
        public virtual async Task<IEnumerable<T>> Get(IEnumerable<SortingField> sortings, Expression<Func<T, object>> projection, CancellationToken token) => (IEnumerable<T>)await GetCollection(null, null, sortings, projection, token);

        public virtual async Task<T> Get(TKey id, CancellationToken token)
        {
            var filter = Builders<IDocument<TKey>>.Filter.Eq("_id", id);
            return await Collection.Find(e => filter.Inject()).SingleAsync(token);
        }

        public virtual async Task Update(T entity, CancellationToken token) => await Collection.ReplaceOneAsync(_ => Equals(_.Id, entity.Id), entity, cancellationToken: token);

        public virtual async Task Update(TKey id, T entity, CancellationToken token) => await Collection.ReplaceOneAsync(_ => Equals(_.Id, id), entity, cancellationToken: token);

        public virtual async Task Update(TKey id, string fieldName, object fieldValue, CancellationToken token)
        {
            var filter = Builders<T>.Filter.Eq(_ => Equals(_.Id, id), true);
            var update = Builders<T>.Update.Set(fieldName, fieldValue);
            await Collection.UpdateOneAsync(filter, update, cancellationToken: token);
        }


        protected virtual async Task<IEnumerable<object>> GetCollection(Expression<Func<T, bool>> filter,
            PagingSettings paging, IEnumerable<SortingField> sortings, Expression<Func<T, object>> projection, CancellationToken token)
        {
            try
            {
                var collection = filter == null ? Collection.Find(_ => true) : Collection.Find(filter);
                var sortingBuilder = new SortDefinitionBuilder<T>();
                var sorts = new List<SortDefinition<T>>();
                if (sortings != null)
                {
                    sorts.AddRange(from sf in sortings
                        where !sf.IsNull
                        select sf.SortingMode == SortingModes.Ascending
                            ? sortingBuilder.Ascending(sf.FieldName)
                            : sortingBuilder.Descending(sf.FieldName));
                }
                collection.Sort(sortingBuilder.Combine(sorts));
                if (projection != null)
                {
                    collection.Project(projection);
                }
                if (paging.IsNull) return (IEnumerable<object>) await collection.ToListAsync(token);
                if (paging.Skip != null) collection = collection.Skip(paging.Skip);
                if (paging.Limit != null) collection = collection.Limit(paging.Limit);

                return (IEnumerable<object>) await collection.ToListAsync(token);

            }
            catch (Exception e)
            {
                throw new Exception(e.Message);
            }
        }



    }
}