using System;
using System.Threading.Tasks;
using MongoDB.Driver;

namespace MongoDistributedLocks
{
    public class MongoLockProvider : ILockProvider
    {
        private readonly IMongoCollection<LockModel> collection;
        private readonly TimeSpan defaultExpirationDelay;
        private readonly TimeSpan acquireRetryDelay;
        private readonly int acquireMaximumAttempts; 

        public MongoLockProvider(IMongoDatabase mongoDatabase, string collectionName = "MongoLockProviderResourceLocks", TimeSpan? defaultExpirationDelay = null, TimeSpan? acquireRetryDelay = null, int acquireMaximumAttempts = int.MaxValue)
        {
            this.defaultExpirationDelay = defaultExpirationDelay ?? TimeSpan.FromMinutes(5);
            this.acquireRetryDelay = acquireRetryDelay ?? TimeSpan.FromMilliseconds(300);
            this.acquireMaximumAttempts = acquireMaximumAttempts;
            // Get our collection
            collection = mongoDatabase.GetCollection<LockModel>(collectionName);
            // Specify a TTL index on the ExpiryPoint field.
            collection.Indexes.CreateOne(new CreateIndexModel<LockModel>(
                Builders<LockModel>.IndexKeys.Ascending(l => l.ExpireAt),
                new CreateIndexOptions
                {
                    ExpireAfter = TimeSpan.Zero,
                }
            ));
        }

        public async Task<IAsyncDisposable> AcquireLock(string resourceId, TimeSpan? expirationDelay = null)
        {
            var lockId = BuildLockId(resourceId);
            var distributedLock = BuildDistributedLockObject(expirationDelay, lockId);
            int tries = 0;
            while (!await distributedLock.AttemptGetLock())
            {
                // If we failed to acquire the lock, wait a moment.
                await Task.Delay(acquireRetryDelay);
                // Stop trying if too many attempts
                if (tries++ > acquireMaximumAttempts)
                {
                    throw new ApplicationException($"Could not acquire lock for {resourceId} within the timeout.");
                }
            }
            return distributedLock;
        }

        public async Task<bool> IsLockAcquired(string resourceId)
        {
            var lockId = BuildLockId(resourceId);
            await using(var distributedLock = BuildDistributedLockObject(TimeSpan.Zero, lockId))
            {
                bool result = await distributedLock.AttemptGetLock();
                return result;
            }
        }

        private static string BuildLockId(string resourceId)
        {
            return $"lock_{resourceId}";
        }

        private MongoDistributedLock BuildDistributedLockObject(TimeSpan? expirationDelay, string lockId)
        {
            return new MongoDistributedLock(collection, lockId, expirationDelay ?? this.defaultExpirationDelay);
        }
    }
}