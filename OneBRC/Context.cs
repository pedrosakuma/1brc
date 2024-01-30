using Microsoft.Win32.SafeHandles;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace OneBRC
{
    internal class Context
    {
        private readonly byte[] Utf8StringUnsafeBuffer = new byte[1024 * 1024];
        private int BufferPosition = 0;
        public readonly IDictionary<Utf8StringUnsafe, Statistics> Keys;
        public readonly ConcurrentQueue<Chunk> ChunkQueue;
        public readonly long BlockSize;
        public readonly SafeFileHandle FileHandle;

        public Context(ConcurrentQueue<Chunk> chunkQueue, SafeFileHandle handle, long blockSize)
            : this(chunkQueue, handle, new Dictionary<Utf8StringUnsafe, Statistics>(262144), blockSize)
        {
        }

        public Context(ConcurrentQueue<Chunk> chunkQueue, SafeFileHandle handle, IDictionary<Utf8StringUnsafe, Statistics> source, long blockSize)
        {
            Keys = source;
            ChunkQueue = chunkQueue;
            BlockSize = 1 << (int)(64 - long.LeadingZeroCount(blockSize));
            FileHandle = handle;
        }


        internal unsafe Statistics GetOrAdd(ref readonly Utf8StringUnsafe key)
        {
            if (!Keys.TryGetValue(key, out var floats))
            {
                ref var buffer = ref MemoryMarshal.GetArrayDataReference(Utf8StringUnsafeBuffer);
                ref var destination = ref Unsafe.Add(ref buffer, BufferPosition);
                Unsafe.CopyBlockUnaligned(ref destination, ref key.PointerRef, (uint)key.Length);
                floats = new Statistics();
                Keys.Add(new Utf8StringUnsafe(ref destination, key.Length), floats);
                BufferPosition += key.Length;
            }
            return floats!;
        }

        internal unsafe Statistics Get(ref readonly Utf8StringUnsafe key)
        {
            return Keys[key];
        }
    }
}
