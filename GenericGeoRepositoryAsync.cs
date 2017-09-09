using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using GH.MongoDb.GenericRepository.Interfaces;
using MongoDB.Driver;
using MongoDB.Driver.GeoJsonObjectModel;

namespace GH.MongoDb.GenericRepository
{
    public abstract class GenericGeoRepositoryAsync<T, TKey> : GenericRepositoryAsync<T, TKey>,
        IGenericGeoRepositoryAsync<T, TKey> where TKey : IEquatable<TKey> where T : IDocument<TKey>, ILocationDocument, new()
    {
        protected GenericGeoRepositoryAsync(IMongoDbConnector connector, string collectionName) : base(connector, collectionName)
        {
        }


        public async Task<IEnumerable<T>> Get(double latitude, double longitude, double distance, Expression<Func<T, bool>> filter, int? skip, int? limit, CancellationToken token)
        {
            FilterDefinition<T> query = null;
            if (distance > 100 && distance < 50000)
            {
                var gp = new GeoJsonPoint<GeoJson2DGeographicCoordinates>(new GeoJson2DGeographicCoordinates(latitude, longitude));
                query = Builders<T>.Filter.Near(s => s.Location, gp, distance);
            }
            query = query != null ? query & filter : filter;

            return await Collection.Find(query).Skip(skip).Limit(limit).ToListAsync(token);

        }

        public async Task<long> Count(double latitude, double longitude, double distance, Expression<Func<T, bool>> filter, CancellationToken token)
        {
            FilterDefinition<T> query = null;
            if (distance > 100 && distance < 50000)
            {
                var gp = new GeoJsonPoint<GeoJson2DGeographicCoordinates>(new GeoJson2DGeographicCoordinates(latitude, longitude));
                query = Builders<T>.Filter.Near(s => s.Location, gp, distance);
            }
            query = query != null ? query & filter : filter;

            return await Collection.CountAsync(query, cancellationToken: token);

        }

    }
}