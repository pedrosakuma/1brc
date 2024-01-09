using System.Numerics;
using System.Text;

namespace OneBRC
{
    public unsafe readonly struct Utf8StringUnsafe : IEquatable<Utf8StringUnsafe>, IComparable<Utf8StringUnsafe>
    {
        private readonly unsafe byte* Pointer;
        private readonly int Length;
        public ReadOnlySpan<byte> Span => new ReadOnlySpan<byte>(Pointer, Length);

        public Utf8StringUnsafe(byte* pointer, int length)
        {
            this.Pointer = pointer;
            this.Length = length;
        }

        public bool Equals(Utf8StringUnsafe other)
        {
            return Span.SequenceEqual(other.Span);
        }

        public override int GetHashCode()
        {
            int length = Length;
            uint hash = (uint)length;
            byte* a = Pointer;

            while (length >= 4)
            {
                hash = (hash + BitOperations.RotateLeft(hash, 5)) ^ *(uint*)a;
                a += 4; length -= 4;
            }
            if (length >= 2)
            {
                hash = (hash + BitOperations.RotateLeft(hash, 5)) ^ *(ushort*)a;
                a += 2; length -= 2;
            }
            if (length > 0)
            {
                hash = (hash + BitOperations.RotateLeft(hash, 5)) ^ *a;
            }
            hash += BitOperations.RotateLeft(hash, 7);
            hash += BitOperations.RotateLeft(hash, 15);
            return (int)hash;
        }

        private static int Compare(Utf8StringUnsafe strA, Utf8StringUnsafe strB)
        {
            int length = Math.Min(strA.Length, strB.Length);

            unsafe
            {
                byte* a = strA.Pointer;
                byte* b = strB.Pointer;

                while (length > 0)
                {
                    if (*a != *b)
                        return *a - *b;
                    a += 1;
                    b += 1;
                    length -= 1;
                }

                // At this point, we have compared all the characters in at least one string.
                // The longer string will be larger.
                // We could optimize and compare lengths before iterating strings, but we want
                // Foo and Foo1 to be sorted adjacent to eachother.
                return strA.Length - strB.Length;
            }
        }

        public int CompareTo(Utf8StringUnsafe other)
        {
            return Compare(this, other);
        }

        public override string ToString()
        {
            return Encoding.UTF8.GetString(Span);
        }
    }
}
