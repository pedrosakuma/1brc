using System.Diagnostics.CodeAnalysis;
using System.Numerics;
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
            ref var pointerRef = ref Unsafe.AsRef<uint>(obj.Pointer);
            ref var vMask = ref Unsafe.As<Mask, uint>(ref Unsafe.Add(ref maskRef, length - 1));

            uint hash1 = Hash1Start;
            uint hash2 = hash1;

            hash1 = (BitOperations.RotateLeft(hash1, 5) + hash1) ^ (pointerRef & vMask);
            hash2 = (BitOperations.RotateLeft(hash2, 5) + hash2) ^ (Unsafe.Add(ref pointerRef, 1) & Unsafe.Add(ref vMask, 1));
            hash1 = (BitOperations.RotateLeft(hash1, 5) + hash1) ^ (Unsafe.Add(ref pointerRef, 2) & Unsafe.Add(ref vMask, 2));
            hash2 = (BitOperations.RotateLeft(hash2, 5) + hash2) ^ (Unsafe.Add(ref pointerRef, 3) & Unsafe.Add(ref vMask, 3));
            //hash1 = (BitOperations.RotateLeft(hash1, 5) + hash1) ^ Unsafe.Add(ref meRef, 4);
            //hash2 = (BitOperations.RotateLeft(hash2, 5) + hash2) ^ Unsafe.Add(ref meRef, 5);
            //hash1 = (BitOperations.RotateLeft(hash1, 5) + hash1) ^ Unsafe.Add(ref meRef, 6);
            //hash2 = (BitOperations.RotateLeft(hash2, 5) + hash2) ^ Unsafe.Add(ref meRef, 7);

            var hash = (int)(hash1 + (hash2 * Factor));
            return hash;
        }
    }
}
