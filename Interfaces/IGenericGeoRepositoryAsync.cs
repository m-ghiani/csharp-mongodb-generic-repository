using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

namespace GH.MongoDb.GenericRepository.Interfaces
{
    public interface IGenericGeoRepositoryAsync<T, in TKey> : IGenericRepositoryAsync<T, TKey>
        where T : IDocument<TKey>, ILocationDocument, new()
        where TKey : IEquatable<TKey>
    {
        Task<long> Count(double latitude, double longitude, double distance, Expression<Func<T, bool>> filter, CancellationToken token);
        Task<IEnumerable<T>> Get(double latitude, double longitude, double distance, Expression<Func<T, bool>> filter, CancellationToken token, int offset = 0, int limit = 0);
    }
}