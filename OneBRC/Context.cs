using System.IO.MemoryMappedFiles;
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

        internal Statistics GetOrAdd(Utf8StringUnsafe key)
        {
            if (!Keys.TryGetValue(key, out var floats))
            {
                var s = Encoding.UTF8.GetString(key.Span);
                floats = new Statistics(s);
                Keys.Add(key, floats);
                Ordered.Insert(~Ordered.BinarySearch(s), s);
            }
            return floats;
        }
    }
}
