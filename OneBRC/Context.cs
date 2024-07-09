using System.IO.MemoryMappedFiles;
using System.Runtime.InteropServices;

namespace OneBRC
{
    internal class Context
    {
        public readonly Dictionary<SmallKey, Statistics> SmallKeys;
        public readonly Dictionary<BigKey, Statistics> BigKeys;
        public readonly SharedState SharedState;
        public readonly MemoryMappedFile MappedFile;

        public Context(SharedState sharedState, MemoryMappedFile mmf)
        {
            SmallKeys = new Dictionary<SmallKey, Statistics>(2048, default(SmallKeyEqualityComparer));
            BigKeys = new Dictionary<BigKey, Statistics>(2048, default(BigKeyEqualityComparer));
            SharedState = sharedState;
            MappedFile = mmf;
        }

        internal Statistics GetOrAdd(ref readonly SmallKey key)
        {
            ref var floats = ref CollectionsMarshal.GetValueRefOrAddDefault(SmallKeys, key, out bool exists);
            if (!exists)
                floats = new Statistics();
            return floats!;
        }
        internal Statistics GetOrAdd(ref readonly BigKey key)
        {
            ref var floats = ref CollectionsMarshal.GetValueRefOrAddDefault(BigKeys, key, out bool exists);
            if (!exists)
                floats = new Statistics();
            return floats!;
        }

    }
}
