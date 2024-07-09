using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OneBRC
{
    internal class SharedState
    {
        public readonly Chunk[] Chunks;
        private int Sequence = -1;
        public SharedState(Chunk[] chunks)
        {
            Chunks = chunks;
        }

        public bool TryGetNextChunk(out Chunk? chunk)
        {
            int nextSequence = Interlocked.Increment(ref Sequence);
            if(nextSequence < Chunks.Length)
            {
                chunk = Chunks[nextSequence];
                return true;
            }
            chunk = null;
            return false;
        }
    }
}
