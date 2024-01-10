using System.Diagnostics.CodeAnalysis;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;

namespace OneBRC
{
    internal readonly struct Utf8StringUnsafeEqualityComparer : IEqualityComparer<Utf8StringUnsafe>
    {
        readonly byte[][] masks;
        public Utf8StringUnsafeEqualityComparer()
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

        public unsafe bool Equals(Utf8StringUnsafe x, Utf8StringUnsafe y)
        {
            return x.Length == y.Length
                && (Unsafe.AsRef<Vector256<byte>>(x.Pointer) & MemoryMarshal.AsRef<Vector256<byte>>(masks[x.Length - 1])) == (Unsafe.AsRef<Vector256<byte>>(y.Pointer) & MemoryMarshal.AsRef<Vector256<byte>>(masks[y.Length - 1]));
        }

        public unsafe int GetHashCode([DisallowNull] Utf8StringUnsafe obj)
        {
            int length = obj.Length;
            var me = Unsafe.AsRef<Vector256<uint>>(obj.Pointer) & MemoryMarshal.AsRef<Vector256<uint>>(masks[length - 1]);
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
    }
}
