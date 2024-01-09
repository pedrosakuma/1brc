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
        private readonly unsafe byte* Pointer;
        private readonly int Length;
        public ReadOnlySpan<byte> Span => new ReadOnlySpan<byte>(Pointer, Length);

        public Utf8StringUnsafe(byte* pointer, int length)
        {
            this.Pointer = pointer;
            this.Length = length;
        }
        static readonly byte[][] masks;
        static Utf8StringUnsafe()
        {
            masks = new byte[32][];
            for (int i = 0; i < masks.Length; i++)
            {
                masks[i] = new byte[32];
                for (int j = 0; j < masks[i].Length; j++)
                {
                    if (j <= i)
                        masks[i][j] = 255;
                }
            }
        }
        
        public bool Equals(Utf8StringUnsafe other)
        {
            return Length == other.Length 
                && (Unsafe.AsRef<Vector256<byte>>(Pointer) & MemoryMarshal.AsRef<Vector256<byte>>(masks[Length - 1])) == (Unsafe.AsRef<Vector256<byte>>(other.Pointer) & MemoryMarshal.AsRef<Vector256<byte>>(masks[other.Length - 1]));
        }

        public override bool Equals([NotNullWhen(true)] object? obj)
        {
            return obj is Utf8StringUnsafe l && Equals(l);
        }

        public override int GetHashCode()
        {
            int length = Length;
            var me = Unsafe.AsRef<Vector256<uint>>(Pointer) & MemoryMarshal.AsRef<Vector256<uint>>(masks[length - 1]);
            uint hash = (uint)length;
            hash = (hash + BitOperations.RotateLeft(hash, 5)) ^ me[0];
            hash = (hash + BitOperations.RotateLeft(hash, 5)) ^ me[1];
            hash = (hash + BitOperations.RotateLeft(hash, 5)) ^ me[2];
            hash = (hash + BitOperations.RotateLeft(hash, 5)) ^ me[3];
            hash = (hash + BitOperations.RotateLeft(hash, 5)) ^ me[4];
            hash = (hash + BitOperations.RotateLeft(hash, 5)) ^ me[5];
            hash = (hash + BitOperations.RotateLeft(hash, 5)) ^ me[6];
            hash = (hash + BitOperations.RotateLeft(hash, 5)) ^ me[7];
            return (int)hash;
        }

        public override string ToString()
        {
            return Encoding.UTF8.GetString(Span);
        }
    }
}
