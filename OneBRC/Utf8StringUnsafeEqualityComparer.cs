using System.Diagnostics.CodeAnalysis;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;

namespace OneBRC
{
    internal readonly struct Utf8StringUnsafeEqualityComparer : IEqualityComparer<Utf8StringUnsafe>
    {
        readonly Vector256<uint>[] masks;
        public Utf8StringUnsafeEqualityComparer()
        {
            var masks = new byte[32][];
            for (int i = 0; i < masks.Length; i++)
            {
                masks[i] = new byte[32];
                for (int j = 0; j < masks[i].Length; j++)
                {
                    if (j <= i)
                        masks[i][j] = 255;
                }
            }
            this.masks = new Vector256<uint>[masks.Length];
            for (int i = 0; i < masks.Length; i++)
                this.masks[i] = MemoryMarshal.AsRef<Vector256<uint>>(masks[i]);
        }

        [SkipLocalsInit]
        public unsafe bool Equals(Utf8StringUnsafe x, Utf8StringUnsafe y)
        {
            uint maskMsb = (1U << (int)x.Length) - 1;
            return (Vector256.Equals(
                Unsafe.AsRef<Vector256<byte>>(x.Pointer), Unsafe.AsRef<Vector256<byte>>(y.Pointer)
            ).ExtractMostSignificantBits() & maskMsb) == maskMsb;
        }

        private const uint Hash1Start = (5381 << 16) + 5381;
        private const uint Factor = 1_566_083_941;
        [SkipLocalsInit]
        public unsafe int GetHashCode([DisallowNull] Utf8StringUnsafe obj)
        {
            uint length = obj.Length;
            ref var maskRef = ref MemoryMarshal.GetArrayDataReference(masks);
            var vPointer = Vector256.Load((uint*)obj.Pointer);
            var vMask = Unsafe.Add(ref maskRef, length - 1);
            var me = Vector256.BitwiseAnd(vPointer, vMask);
            ref var meRef = ref Unsafe.As<Vector256<uint>, uint>(ref me);

            uint hash1 = Hash1Start;
            uint hash2 = hash1;

            hash1 = (BitOperations.RotateLeft(hash1, 5) + hash1) ^ meRef;
            hash2 = (BitOperations.RotateLeft(hash2, 5) + hash2) ^ Unsafe.Add(ref meRef, 1);
            hash1 = (BitOperations.RotateLeft(hash1, 5) + hash1) ^ Unsafe.Add(ref meRef, 2);
            hash2 = (BitOperations.RotateLeft(hash2, 5) + hash2) ^ Unsafe.Add(ref meRef, 3);
            //hash1 = (BitOperations.RotateLeft(hash1, 5) + hash1) ^ Unsafe.Add(ref meRef, 4);
            //hash2 = (BitOperations.RotateLeft(hash2, 5) + hash2) ^ Unsafe.Add(ref meRef, 5);
            //hash1 = (BitOperations.RotateLeft(hash1, 5) + hash1) ^ Unsafe.Add(ref meRef, 6);
            //hash2 = (BitOperations.RotateLeft(hash2, 5) + hash2) ^ Unsafe.Add(ref meRef, 7);

            var hash = (int)(hash1 + (hash2 * Factor));
            return hash;
        }
    }
}
