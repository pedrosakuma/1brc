using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
