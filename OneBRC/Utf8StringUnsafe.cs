using System.Diagnostics.CodeAnalysis;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Text;

namespace OneBRC
{
    public unsafe readonly struct Utf8StringUnsafe : IEquatable<Utf8StringUnsafe>
    {
        static readonly Utf8StringUnsafeEqualityComparer comparer = new Utf8StringUnsafeEqualityComparer();
        internal readonly unsafe byte* Pointer;
        internal readonly int Length;
        public ReadOnlySpan<byte> Span => new ReadOnlySpan<byte>(Pointer, Length);

        public Utf8StringUnsafe(byte* pointer, int length)
        {
            this.Pointer = pointer;
            this.Length = length;
        }
        
        public bool Equals(Utf8StringUnsafe other)
        {
            return comparer.Equals(this, other);
        }

        public override bool Equals([NotNullWhen(true)] object? obj)
        {
            return obj is Utf8StringUnsafe l && Equals(l);
        }

        public override int GetHashCode()
        {
            return comparer.GetHashCode(this);
        }

        public override string ToString()
        {
            return Encoding.UTF8.GetString(Span);
        }
    }
}
