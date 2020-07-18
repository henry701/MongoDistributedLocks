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

        public MongoLockProvider(IMongoDatabase mongoDatabase, TimeSpan defaultExpirationDelay, TimeSpan acquireRetryDelay, int acquireMaximumAttempts = int.MaxValue)
        {
            this.acquireRetryDelay = acquireRetryDelay;
            this.acquireMaximumAttempts = acquireMaximumAttempts;
            // Get our collection
            collection = mongoDatabase.GetCollection<LockModel>("resourceLocks");
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
            // Determine the id of the lock
            var lockId = $"lock_{resourceId}";

            var distributedLock = new MongoDistributedLock(collection, lockId, expirationDelay ?? this.defaultExpirationDelay);

            int tries = 0;
            // Try and acquire the lock
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

            // This will only return if we have the lock.
            return distributedLock;
        }
    }
}