using System.Diagnostics.CodeAnalysis;
using System.Numerics;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;

namespace OneBRC
{
    internal readonly struct Utf8StringUnsafeEqualityComparer : IEqualityComparer<Utf8StringUnsafe>
    {
        [InlineArray(8)]
        private struct Mask
        {
            public uint Data;
            public override string ToString()
            {
                ref var refThis = ref Unsafe.As<Mask, uint>(ref this);

                return $"{{{refThis}, {Unsafe.Add(ref refThis, 1)}, {Unsafe.Add(ref refThis, 2)}, {Unsafe.Add(ref refThis, 3)}, {Unsafe.Add(ref refThis, 4)}, {Unsafe.Add(ref refThis, 5)}, {Unsafe.Add(ref refThis, 6)} , {Unsafe.Add(ref refThis, 7)}}}";
            }
        }

        readonly Mask[] masks;
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
            this.masks = new Mask[masks.Length];
            for (int i = 0; i < masks.Length; i++)
                this.masks[i] = MemoryMarshal.AsRef<Mask>(masks[i]);
        }

        [SkipLocalsInit]
        public unsafe bool Equals(Utf8StringUnsafe x, Utf8StringUnsafe y)
        {
            bool equals = true;
            ref var xRef = ref Unsafe.AsRef<Vector256<byte>>(x.Pointer);
            ref var yRef = ref Unsafe.AsRef<Vector256<byte>>(y.Pointer);
            int xLength = (int)x.Length;
            while (equals && xLength >= 32)
            {
                equals = Vector256.EqualsAll(xRef, yRef);
                xRef = ref Unsafe.Add(ref xRef, 1);
                yRef = ref Unsafe.Add(ref yRef, 1);
                xLength -= Vector256<byte>.Count;
            }
            uint maskMsb = (1U << (int)xLength) - 1;
            return equals && (Vector256.Equals(xRef, yRef)
                .ExtractMostSignificantBits() & maskMsb) == maskMsb;
        }

        private const uint Hash1Start = (5381 << 16) + 5381;
        private const uint Factor = 1_566_083_941;
        [SkipLocalsInit]
        public unsafe int GetHashCode([DisallowNull] Utf8StringUnsafe obj)
        {
            int length = (int)obj.Length;
            uint hash1, hash2;
            hash1 = Hash1Start;
            hash2 = (BitOperations.RotateLeft(hash1, 5) + hash1) ^ obj.Length;

            ref var r = ref Unsafe.AsRef<uint>(obj.Pointer);
            while (length >= 8)
            {
                hash1 = (BitOperations.RotateLeft(hash1, 5) + hash1) ^ r;
                hash2 = (BitOperations.RotateLeft(hash2, 5) + hash2) ^ Unsafe.Add(ref r, 1);
                r = ref Unsafe.Add(ref r, 2);
                length -= 8;
            }
            ref var b = ref Unsafe.As<uint, byte>(ref r);
            while (length-- > 0)
            {
                hash2 = (BitOperations.RotateLeft(hash2, 5) + hash2) ^ Unsafe.Add(ref b, length + 1);
            }
            return (int)(hash1 + (hash2 * Factor));
        }
    }
}
