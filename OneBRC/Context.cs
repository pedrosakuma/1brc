using System.Runtime.InteropServices;
using System.Text;

namespace OneBRC
{
    internal class Context
    {
        public Dictionary<int, string> keys;
        public Dictionary<string, Statistics> data = new Dictionary<string, Statistics>(512);
        public List<string> ordered;

        public int BlockBufferSize;
        public readonly byte[] BlockBuffer;
        public int LinesCount;
        public readonly int[] Indexes;
        public readonly int[] Lengths;
        public Context(Dictionary<int, string> keys, List<string> ordered)
        {
            this.keys = keys;
            this.ordered = ordered;

            data = new Dictionary<string, Statistics>(512);
            BlockBuffer = new byte[524288];
            Indexes = new int[131072] ;
            Lengths = new int[131072] ;
        }

        internal Statistics GetOrAdd(ReadOnlySpan<byte> span)
        {
            int keyHashCode = GetHashCode(span);
            Statistics? floats;
            if (!keys.TryGetValue(keyHashCode, out var key))
            {
                key = Encoding.UTF8.GetString(span);
                keys.Add(keyHashCode, key);
                ordered.Insert(~ordered.BinarySearch(key), key);
            }
            if (!data.TryGetValue(key, out floats))
            {
                floats = new Statistics();
                data.Add(key, floats);
            }

            return floats;
        }

        static int GetHashCode(ReadOnlySpan<byte> span)
        {
            int hashCode = 0;
            for (int i = 0; i < span.Length; i++)
                hashCode = 31 * hashCode + span[i];
            return hashCode;
        }

    }
}
