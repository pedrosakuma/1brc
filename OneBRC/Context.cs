using System.Collections.Concurrent;
using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace OneBRC
{
    internal class Context
    {
        public readonly Dictionary<int, Statistics> IntSizeKeys;
        public readonly Dictionary<long, Statistics> LongSizeKeys;
        public readonly Dictionary<Utf8StringUnsafe, Statistics> Keys;
        public readonly ConcurrentQueue<Chunk> ChunkQueue;
        public readonly MemoryMappedFile MappedFile;

        public Context(ConcurrentQueue<Chunk> chunkQueue, MemoryMappedFile mmf)
        {
            IntSizeKeys = new Dictionary<int, Statistics>(262144);
            LongSizeKeys = new Dictionary<long, Statistics>(262144);
            Keys = new Dictionary<Utf8StringUnsafe, Statistics>(262144);
            ChunkQueue = chunkQueue;
            MappedFile = mmf;
        }

        internal unsafe Statistics GetOrAdd(ref readonly Utf8StringUnsafe key)
        {
            //ref Statistics? floats = ref Unsafe.NullRef<Statistics?>();
            bool exists = false;
            //switch (key.Length)
            //{
            //    case 1:
            //    case 2:
            //    case 3:
            //        {
            //            var keyValue = Unsafe.ReadUnaligned<int>(key.Pointer) & (1 << (key.Length * 8)) - 1;
            //            floats = ref CollectionsMarshal.GetValueRefOrAddDefault(IntSizeKeys, keyValue, out exists);
            //            break;
            //        }
            //    case 4:
            //        {
            //            var keyValue = Unsafe.ReadUnaligned<int>(key.Pointer);
            //            floats = ref CollectionsMarshal.GetValueRefOrAddDefault(IntSizeKeys, keyValue, out exists);
            //            break;
            //        }
            //    case 5:
            //    case 6:
            //    case 7:
            //        {
            //            var keyValue = Unsafe.ReadUnaligned<long>(key.Pointer) & (1L << (key.Length * 8)) - 1;
            //            floats = ref CollectionsMarshal.GetValueRefOrAddDefault(LongSizeKeys, keyValue, out exists);
            //            break;
            //        }
            //    case 8:
            //        {
            //            var keyValue = Unsafe.ReadUnaligned<long>(key.Pointer);
            //            floats = ref CollectionsMarshal.GetValueRefOrAddDefault(LongSizeKeys, keyValue, out exists);
            //            break;
            //        }
            //    default:
            //        floats = ref CollectionsMarshal.GetValueRefOrAddDefault(Keys, key, out exists);
            //        break;
            //}
            ref var floats = ref CollectionsMarshal.GetValueRefOrAddDefault(Keys, key, out exists);
            if (!exists)
                floats = new Statistics();
            return floats!;
        }
    }
}
