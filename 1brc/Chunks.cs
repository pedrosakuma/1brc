using System.Diagnostics.CodeAnalysis;

namespace OneBRC
{
    internal class Chunks
    {
        private int index;
        readonly Chunk[] chunks;
        public Chunks(Chunk[] chunks)
        {
            this.chunks = chunks;
        }

        public bool TryGetNext([NotNullWhen(true)]out Chunk? chunk)
        {
            var currentIndex = Interlocked.Increment(ref index);
            if (currentIndex < chunks.Length)
            {
                chunk = chunks[currentIndex];
                return true;
            }
            else
            {
                chunk = default;
                return false;
            }
        }
    }
}
