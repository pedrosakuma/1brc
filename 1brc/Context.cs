using System.Collections.Concurrent;
using System.Collections.Frozen;
using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;

namespace OneBRC
{
    internal class Context
    {
        public readonly FrozenDictionary<long, Statistics> SmallKeys;
        public readonly FrozenDictionary<Utf8StringUnsafe, Statistics> Keys;
        public readonly ConcurrentQueue<Chunk> ChunkQueue;
        public readonly MemoryMappedFile MappedFile;

        public Context(ConcurrentQueue<Chunk> chunkQueue, MemoryMappedFile mmf, FrozenDictionary<long, Statistics> smallKeys, FrozenDictionary<Utf8StringUnsafe, Statistics> keys)
        {
            ChunkQueue = chunkQueue;
            MappedFile = mmf;
            SmallKeys = smallKeys;
            Keys = keys;
        }

        internal unsafe Statistics Get(ref readonly Utf8StringUnsafe key)
        {
            switch (key.Length)
            {
                case 1:
                case 2:
                case 3:
                case 4:
                case 5:
                case 6:
                case 7:
                    {
                        long smallKey = Unsafe.ReadUnaligned<long>(ref key.PointerRef) & (1L << (key.Length * 8)) - 1;
                        return SmallKeys[smallKey];
                    }
                case 8:
                    {
                        long smallKey = Unsafe.ReadUnaligned<long>(ref key.PointerRef);
                        return SmallKeys[smallKey];
                    }
                default:
                    return Keys[key];
            }
        }
    }
}
