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
            Keys = new Dictionary<Utf8StringUnsafe, Statistics>(512);
            Ordered = new List<string>(512);
            MemoryMappedFile = mmf;
            Position = position;
            Size = size;
        }

        internal ref Statistics GetOrAdd(Utf8StringUnsafe key)
        {
            ref Statistics? floats = ref CollectionsMarshal.GetValueRefOrAddDefault(Keys, key, out bool exists);
            if(!exists)
            {
                floats = new Statistics(key);
                var s = Encoding.UTF8.GetString(key.Span);
                Ordered.Insert(~Ordered.BinarySearch(s), s);
            }
#pragma warning disable CS8619 // Nullability of reference types in value doesn't match target type.
            return ref floats;
#pragma warning restore CS8619 // Nullability of reference types in value doesn't match target type.
        }
    }
}
