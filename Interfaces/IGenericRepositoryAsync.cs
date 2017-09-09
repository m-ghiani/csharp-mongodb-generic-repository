using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

namespace GH.MongoDb.GenericRepository.Interfaces
{
    public interface IGenericRepositoryAsync<T, in TKey> where T : IDocument<TKey>, new() where TKey : IEquatable<TKey>
    {
        IMongoDbConnector Connector { get; }
        Task Add(T entity, CancellationToken token);
        Task Add(List<T> entities, CancellationToken token);
        Task<long> Count(Expression<Func<T, bool>> filter, CancellationToken token);
        Task Delete(TKey id, CancellationToken token);
        Task DropCollection(CancellationToken token);
        Task<bool> Exist(Expression<Func<T, bool>> filter, CancellationToken token);
        Task<bool> ExistCollection(CancellationToken token);
        Task<IEnumerable<T>> Get(CancellationToken token);
        Task<IEnumerable<T>> Get(int? skip, CancellationToken token);
        Task<IEnumerable<T>> Get(int? skip, int? limit, CancellationToken token);
        Task<IEnumerable<T>> Get(Expression<Func<T, bool>> filter, CancellationToken token);
        Task<IEnumerable<T>> Get(Expression<Func<T, bool>> filter, int? limit, CancellationToken token);
        Task<IEnumerable<T>> Get(Expression<Func<T, bool>> filter, int? skip, int? limit, CancellationToken token);
        Task<T> Get(TKey id, CancellationToken token);
        Task Update(T entity, CancellationToken token);
        Task Update(TKey id, T entitty, CancellationToken token);
        Task Update(TKey id, string fieldName, object fieldValue, CancellationToken token);
    }
}