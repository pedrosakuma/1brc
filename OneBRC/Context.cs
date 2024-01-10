using System.IO.MemoryMappedFiles;
using System.Runtime.InteropServices;
using System.Text;

namespace OneBRC
{
    internal class Context
    {
        public readonly MemoryMappedFile MemoryMappedFile;
        public readonly Dictionary<Utf8StringUnsafe, Statistics> Keys;
        public readonly List<string> Ordered;
        public long Position { get; }
        public int Size { get; }

        public Context(MemoryMappedFile mmf, long position, int size)
        {
            Keys = new Dictionary<Utf8StringUnsafe, Statistics>(32768);
            Ordered = new List<string>(512);
            MemoryMappedFile = mmf;
            Position = position;
            Size = size;
        }

        internal ref Statistics GetOrAdd(Utf8StringUnsafe key)
        {
            ref var floats = ref CollectionsMarshal.GetValueRefOrAddDefault(Keys, key, out bool exists);
            if (!exists)
            {
                var s = Encoding.UTF8.GetString(key.Span);
                floats = new Statistics(s);
                Ordered.Insert(~Ordered.BinarySearch(s), s);
            }
            return ref floats!;
        }
    }
}
