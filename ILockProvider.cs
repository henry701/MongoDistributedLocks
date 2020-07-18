
using System;
using System.Threading.Tasks;

namespace MongoDistributedLocks
{
    public interface ILockProvider
    {
        Task<IAsyncDisposable> AcquireLock(string resourceId, TimeSpan? expirationDelay = null);
    }
}