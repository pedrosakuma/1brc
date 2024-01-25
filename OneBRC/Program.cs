using System.Buffers;
using System.Collections.Concurrent;
using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;

namespace OneBRC;

class Program
{
    static readonly SearchValues<byte> LineBreakAndComma = SearchValues.Create(";\n"u8);

    static unsafe void Main(string[] args)
    {
        string path = args[0].Replace("~", Environment.GetFolderPath(Environment.SpecialFolder.UserProfile));
        #if DEBUG
        int parallelism = 1;
        #else
        int parallelism = Environment.ProcessorCount;
        #endif
        int chunks = Environment.ProcessorCount * 2000;
        Console.WriteLine($"Parallelism: {parallelism}");
        Console.WriteLine($"Chunks: {chunks}");
        Console.WriteLine($"Vector512.IsHardwareAccelerated: {Vector512.IsHardwareAccelerated}");
        Console.WriteLine($"Vector256.IsHardwareAccelerated: {Vector256.IsHardwareAccelerated}");
        long length = GetFileLength(path);

        var contexts = new Context[parallelism];
        var consumers = new Thread[parallelism];

        ConcurrentQueue<Chunk> chunkQueue;
        using (var fileHandle = File.OpenHandle(path, FileMode.Open, FileAccess.Read, FileShare.Read, FileOptions.RandomAccess))
        using (var mmf = MemoryMappedFile.CreateFromFile(fileHandle, null, 0, MemoryMappedFileAccess.Read, HandleInheritability.None, true))
        {
            chunkQueue = new ConcurrentQueue<Chunk>(
                CreateChunks(mmf, chunks, length)
            );
            for (int i = 0; i < parallelism; i++)
            {
                int index = i;
                contexts[i] = new Context(chunkQueue, mmf);
                if (Vector512.IsHardwareAccelerated)
                    consumers[i] = new Thread(ConsumeVector512);
                else if (Vector256.IsHardwareAccelerated)
                    consumers[i] = new Thread(ConsumeVector256);
                else
                    consumers[i] = new Thread(ConsumeSlow);
                consumers[i].Start(contexts[i]);
            }
            foreach (var consumer in consumers)
                consumer.Join();

            WriteOrderedStatistics(GroupAndAggregateStatistics(contexts));
        }
    }

    private static unsafe Chunk[] CreateChunks(MemoryMappedFile mmf, int chunks, long length)
    {
        var result = new List<Chunk>();
        long blockSize = length / (long)chunks;

        using (var va = mmf.CreateViewAccessor(0, 0, MemoryMappedFileAccess.Read))
        {
            long position = 0;
            byte* ptr = (byte*)0;
            va.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);

            while (true)
            {
                long remainder = length - position;
                byte* ptrBlock = ptr + position;
                checked
                {
                    int size = (int)long.Min(blockSize, remainder);
                    if (size == 0)
                        break;

                    var span = new ReadOnlySpan<byte>(ptrBlock, size);
                    int lastIndexOfLineBreak = span.LastIndexOf((byte)'\n');
                    if (lastIndexOfLineBreak == -1)
                        break;

                    result.Add(new Chunk(position, lastIndexOfLineBreak));
                    position += (long)lastIndexOfLineBreak + 1;
                }
            }
        }
        return result.ToArray();
    }


    private static long GetFileLength(string path)
    {
        using (var file = File.OpenRead(path))
            return file.Length;
    }

    private static void WriteOrderedStatistics(Dictionary<string, Statistics> final)
    {
        bool first = true;
        var c = Console.Out;
        c.Write("{");
        foreach (var item in final.Keys.Order())
        {
            Statistics statistics = final[item];
            if (first)
                first = false;
            else
                c.Write(", ");

            c.Write($"{item}={(statistics.Min / 10f).ToString("0.0")}/{(float)(statistics.Sum / 10f) / statistics.Count:0.0}/{(statistics.Max / 10f).ToString("0.0")}");
        }
        c.WriteLine("}");
        c.Flush();
    }

    private static Dictionary<string, Statistics> GroupAndAggregateStatistics(Context[] contexts)
    {
        Dictionary<string, Statistics> final = new Dictionary<string, Statistics>(32768);
        foreach (var context in contexts)
        {
            foreach (var data in context.Keys)
            {
                string key = data.Key.ToString();
                if (!final.TryGetValue(key, out var stats))
                {
                    stats = new Statistics();
                    final.Add(key, stats);
                }
                stats.Count += data.Value.Count;
                stats.Sum += data.Value.Sum;
                stats.Min = int.Min(stats.Min, data.Value.Min);
                stats.Max = int.Max(stats.Max, data.Value.Max);
            }
        }
        return final;
    }

    private unsafe static void ConsumeSlow(object? obj)
    {
        ArgumentNullException.ThrowIfNull(obj);
        Context context = (Context)obj;

        using (var va = context.MappedFile.CreateViewAccessor(0, 0, MemoryMappedFileAccess.Read))
        {
            byte* ptr = (byte*)0;
            va.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);
            while (context.ChunkQueue.TryDequeue(out var chunk))
                ConsumeSlow(context, ptr + chunk.Position, chunk.Size);
        }
    }
    private unsafe static void ConsumeVector256(object? obj)
    {
        ArgumentNullException.ThrowIfNull(obj);
        Context context = (Context)obj;

        using (var va = context.MappedFile.CreateViewAccessor(0, 0, MemoryMappedFileAccess.Read))
        {
            byte* ptr = (byte*)0;
            va.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);
            while (context.ChunkQueue.TryDequeue(out var chunk))
                ConsumeWithVector256(context, ref Unsafe.AsRef<byte>(ptr + chunk.Position), chunk.Size);
        }
    }
    private unsafe static void ConsumeVector512(object? obj)
    {
        ArgumentNullException.ThrowIfNull(obj);
        Context context = (Context)obj;

        using (var va = context.MappedFile.CreateViewAccessor(0, 0, MemoryMappedFileAccess.Read))
        {
            byte* ptr = (byte*)0;
            va.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);
            while (context.ChunkQueue.TryDequeue(out var chunk))
                ConsumeWithVector512(context, ref Unsafe.AsRef<byte>(ptr + chunk.Position), chunk.Size);
        }
    }

    private static unsafe void ConsumeSlow(Context context, byte* ptr, int size)
    {
        Utf8StringUnsafe[] data = new Utf8StringUnsafe[16];
        ref var dataRef = ref MemoryMarshal.GetArrayDataReference(data);

        ref byte searchSpace = ref Unsafe.AsRef<byte>(ptr);

        ref byte currentSearchSpace = ref searchSpace;
        ref byte end = ref Unsafe.Add(ref searchSpace, size);
        SerialRemainder(context, ref dataRef, 0, ref currentSearchSpace, ref end);
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private unsafe static void ConsumeWithVector512(Context context, ref byte searchSpace, int size)
    {
        int[] indexes = new int[Vector512<int>.Count * sizeof(int)];
        ref int indexesRef = ref indexes[0];
        ref int indexesPlusOneRef = ref indexes[1];

        ref byte currentSearchSpace = ref searchSpace;
        ref byte end = ref Unsafe.Add(ref searchSpace, size);
        ref byte oneVectorAwayFromEnd = ref Unsafe.Subtract(ref end, Vector512<byte>.Count);

        int count;
        while ((count = ExtractIndexesVector512(ref currentSearchSpace, ref oneVectorAwayFromEnd, ref indexesPlusOneRef)) == Vector512<int>.Count)
        {
            var add = Vector512.Create(0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1);
            ref var indexesVectorRef = ref Unsafe.As<int, Vector512<int>>(ref indexesRef);

            var addressesVectorRef = indexesVectorRef + add;
            var sizesVectorRef = Unsafe.As<int, Vector512<int>>(ref indexesPlusOneRef) - indexesVectorRef - add;

            var (lowAddressOffset, highAddressOffset) = Vector512.Widen(addressesVectorRef);
            var (lowSizes, highSizes) = Vector512.Widen(sizesVectorRef);

            var currentSearchSpaceAddressVector = Vector512.Create((long)(nint)Unsafe.AsPointer(ref currentSearchSpace));

            Vector512<long> lowAddress = lowAddressOffset + currentSearchSpaceAddressVector;
            GetOrAddUnpackedParts(context, ref lowAddress, ref lowSizes);

            Vector512<long> highAddress = highAddressOffset + currentSearchSpaceAddressVector;
            GetOrAddUnpackedParts(context, ref highAddress, ref highSizes);

            uint lastIndex = (uint)(Unsafe.Add(ref Unsafe.As<Vector512<int>, int>(ref addressesVectorRef), count - 1) + Unsafe.Add(ref Unsafe.As<Vector512<int>, int>(ref sizesVectorRef), count - 1) + 1);
            currentSearchSpace = ref Unsafe.Add(ref currentSearchSpace, lastIndex);
        }

        Utf8StringUnsafe[] data = new Utf8StringUnsafe[16];
        ref var dataRef = ref MemoryMarshal.GetArrayDataReference(data);
        int dataIndex = 0;
        SerialRemainder(context, ref dataRef, dataIndex, ref currentSearchSpace, ref end);
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
    private static unsafe void GetOrAddUnpackedParts(Context context, ref readonly Vector512<long> addresses, ref readonly Vector512<long> sizes)
    {
        var lowAddressesAndSizes = Avx512F.UnpackLow(addresses, sizes);
        var highAddressesAndSizes = Avx512F.UnpackHigh(addresses, sizes);

        ref var lowStringUnsafe = ref Unsafe.As<Vector512<long>, Utf8StringUnsafe>(ref lowAddressesAndSizes);
        ref var highStringUnsafe = ref Unsafe.As<Vector512<long>, Utf8StringUnsafe>(ref highAddressesAndSizes);

        ParseQuadFixedPoint(Vector256.Create(
            *(long*)highStringUnsafe.Pointer,
            *(long*)Unsafe.Add(ref highStringUnsafe, 1).Pointer,
            *(long*)Unsafe.Add(ref highStringUnsafe, 2).Pointer,
            *(long*)Unsafe.Add(ref highStringUnsafe, 3).Pointer), out Vector256<short> fixedPointTemps);

        context.GetOrAdd(ref lowStringUnsafe)
            .Add(fixedPointTemps[0]);
        context.GetOrAdd(ref Unsafe.Add(ref lowStringUnsafe, 1))
            .Add(fixedPointTemps[4]);
        context.GetOrAdd(ref Unsafe.Add(ref lowStringUnsafe, 2))
            .Add(fixedPointTemps[8]);
        context.GetOrAdd(ref Unsafe.Add(ref lowStringUnsafe, 3))
            .Add(fixedPointTemps[12]);
    }

    static readonly Vector256<byte> s_quadFixedPointLeftAlignShuffle = Vector256.Create(
        (byte)0, 2, 3, 4, 3, 2, 1, 0,
        8, 10, 11, 12, 11, 10, 9, 8,
        16, 18, 19, 20, 19, 18, 17, 16,
        24, 26, 27, 28, 27, 26, 25, 24);

    static readonly Vector128<byte> s_quadFixedPointLeftAlignShuffle128 = Vector128.Create(
        (byte)0, 2, 3, 4, 3, 2, 1, 0,
        8, 10, 11, 12, 11, 10, 9, 8);
    static readonly Vector256<byte> s_dotMask = Vector256.Create(
        0, (byte)'.', (byte)'.', 0, 0, 0, 0, 0,
        0, (byte)'.', (byte)'.', 0, 0, 0, 0, 0,
        0, (byte)'.', (byte)'.', 0, 0, 0, 0, 0,
        0, (byte)'.', (byte)'.', 0, 0, 0, 0, 0);
    static readonly Vector256<sbyte> s_dotMult = Vector256.Create(
        3, 2, 0, 0, 0, 0, 0, 0,
        3, 2, 0, 0, 0, 0, 0, 0,
        3, 2, 0, 0, 0, 0, 0, 0,
        3, 2, 0, 0, 0, 0, 0, 0);
    static readonly Vector256<sbyte> s_fixedPointMult1LeftAligned = Vector256.Create(
        1, 0, 10, 100, 0, 0, 0, 0,
        1, 0, 10, 100, 0, 0, 0, 0,
        1, 0, 10, 100, 0, 0, 0, 0,
        1, 0, 10, 100, 0, 0, 0, 0);

    [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
    protected static void ParseQuadFixedPoint(Vector256<long> tempUtf8Bytes, out Vector256<short> fixedPointTemps)
    {
        Vector256<byte> v = Avx2.Shuffle(tempUtf8Bytes.AsByte(), s_quadFixedPointLeftAlignShuffle);
        Vector256<byte> dashMask = Vector256.Create((long)'-').AsByte();
        Vector256<byte> dashes = Vector256.Equals(v, dashMask);
        Vector256<int> negMask = Vector256.ShiftRightArithmetic(Vector256.ShiftLeft(dashes.AsInt32(), 24), 24);
        Vector256<byte> dots = Avx2.ShiftRightLogical(Vector256.Equals(v, s_dotMask).AsInt64(), 8).AsByte();
        Vector256<long> dotPositions = Avx2.And(Avx2.MultiplyAddAdjacent(dots, s_dotMult).AsInt64(), Vector256.Create(3L));
        Vector256<ulong> shifts = Avx2.ShiftLeftLogical(Vector256.Create(5L) - dotPositions, 3).AsUInt64();
        Vector256<byte> alignedV = Avx2.ShiftRightLogicalVariable(v.AsInt64(), shifts).AsByte();
        Vector256<byte> digits = Avx2.SubtractSaturate(alignedV, Vector256.Create<byte>((byte)'0'));
        Vector256<short> partialSums = Avx2.MultiplyAddAdjacent(digits, s_fixedPointMult1LeftAligned);
        Vector256<short> absFixedPoint = Vector256.Add(Avx2.ShiftRightLogical(partialSums.AsInt32(), 16).AsInt16(), partialSums);
        var negFixedPoint = -absFixedPoint;
        fixedPointTemps = Avx2.BlendVariable(absFixedPoint, negFixedPoint, negMask.AsInt16());
    }

    private static unsafe void ConsumeWithVector256(Context context, ref byte searchSpace, int size)
    {
        int[] indexes = new int[sizeof(int) * 8];
        ref int indexesRef = ref indexes[0];
        ref int indexesPlusOneRef = ref indexes[1];

        ref byte currentSearchSpace = ref searchSpace;
        ref byte end = ref Unsafe.Add(ref searchSpace, size);
        ref byte oneVectorAwayFromEnd = ref Unsafe.Subtract(ref end, Vector256<byte>.Count);

        int count;
        while ((count = ExtractIndexesVector256(ref currentSearchSpace, ref oneVectorAwayFromEnd, ref indexesPlusOneRef)) == Vector256<int>.Count)
        {
            var add = Vector256.Create(0, 1, 1, 1, 1, 1, 1, 1);
            var indexesVectorRef = Unsafe.As<int, Vector256<int>>(ref indexesRef);
            var addressesVectorRef = indexesVectorRef + add;
            var sizesVectorRef = Unsafe.As<int, Vector256<int>>(ref indexesPlusOneRef) - indexesVectorRef - add;

            var (lowAddressesOffset, highAddressesOffset) = Vector256.Widen(addressesVectorRef);
            var (lowSizes, highSizes) = Vector256.Widen(sizesVectorRef);

            var currentSearchSpaceAddressVector = Vector256.Create((long)(nint)Unsafe.AsPointer(ref currentSearchSpace));

            var lowAddresses = lowAddressesOffset + currentSearchSpaceAddressVector;
            var lowLowAddressesAndSizes = Avx2.UnpackLow(lowAddresses, lowSizes);
            var lowHighAddressesAndSizes = Avx2.UnpackHigh(lowAddresses, lowSizes);

            ref var lowLowStringUnsafe = ref Unsafe.As<Vector256<long>, Utf8StringUnsafe>(ref lowLowAddressesAndSizes);
            ref var lowHighStringUnsafe = ref Unsafe.As<Vector256<long>, Utf8StringUnsafe>(ref lowHighAddressesAndSizes);

            var highAddresses = highAddressesOffset + currentSearchSpaceAddressVector;

            var highLowAddressesAndSizes = Avx2.UnpackLow(highAddresses, highSizes);
            var highHighAddressesAndSizes = Avx2.UnpackHigh(highAddresses, highSizes);

            ref var highLowStringUnsafe = ref Unsafe.As<Vector256<long>, Utf8StringUnsafe>(ref highLowAddressesAndSizes);
            ref var highHighStringUnsafe = ref Unsafe.As<Vector256<long>, Utf8StringUnsafe>(ref highHighAddressesAndSizes);

            ParseQuadFixedPoint(Vector256.Create(
                *(long*)lowHighStringUnsafe.Pointer,
                *(long*)Unsafe.Add(ref lowHighStringUnsafe, 1).Pointer,
                *(long*)highHighStringUnsafe.Pointer,
                *(long*)Unsafe.Add(ref highHighStringUnsafe, 1).Pointer), out Vector256<short> fixedPointTemps);

            context.GetOrAdd(ref lowHighStringUnsafe)
                .Add(fixedPointTemps[0]);
            context.GetOrAdd(ref Unsafe.Add(ref lowHighStringUnsafe, 1))
                .Add(fixedPointTemps[4]);
            context.GetOrAdd(ref highHighStringUnsafe)
                .Add(fixedPointTemps[8]);
            context.GetOrAdd(ref Unsafe.Add(ref highHighStringUnsafe, 1))
                .Add(fixedPointTemps[12]);

            uint lastIndex = (uint)(Unsafe.Add(ref Unsafe.As<Vector256<int>, int>(ref addressesVectorRef), count - 1) + Unsafe.Add(ref Unsafe.As<Vector256<int>, int>(ref sizesVectorRef), count - 1) + 1);
            currentSearchSpace = ref Unsafe.Add(ref currentSearchSpace, lastIndex);
        }
        Utf8StringUnsafe[] data = new Utf8StringUnsafe[16];
        ref var dataRef = ref MemoryMarshal.GetArrayDataReference(data);
        int dataIndex = 0;

        SerialRemainder(context, ref dataRef, dataIndex, ref currentSearchSpace, ref end);
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
    private static int ExtractIndexesVector256(ref byte start, ref byte end, ref int indexesPlusOneRef)
    {
        ref var currentSearchSpace = ref Unsafe.As<byte, Vector256<byte>>(ref start);
        ref var oneVectorAwayFromEnd = ref Unsafe.As<byte, Vector256<byte>>(ref end);
        int index = 0;
        int count = 0;
        while(!Unsafe.IsAddressGreaterThan(ref currentSearchSpace, ref oneVectorAwayFromEnd)
            && count < Vector256<int>.Count)
        {
            uint mask = Vector256.BitwiseOr(
                Vector256.Equals(currentSearchSpace, Vector256.Create((byte)'\n')),
                Vector256.Equals(currentSearchSpace, Vector256.Create((byte)';'))
            ).ExtractMostSignificantBits();
            count += mask.ExtractIndexes(ref Unsafe.Add(ref indexesPlusOneRef, count), index);
            currentSearchSpace = ref Unsafe.Add(ref currentSearchSpace, 1);
            index += Vector256<byte>.Count;
        }
        return int.Min(count, Vector256<int>.Count);
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
    private static int ExtractIndexesVector512(ref byte start, ref byte end, ref int indexesPlusOneRef)
    {
        ref var currentSearchSpace = ref Unsafe.As<byte, Vector512<byte>>(ref start);
        ref var oneVectorAwayFromEnd = ref Unsafe.As<byte, Vector512<byte>>(ref end);
        int index = 0;
        int count = 0;
        while (!Unsafe.IsAddressGreaterThan(ref currentSearchSpace, ref oneVectorAwayFromEnd)
            && count < Vector512<int>.Count)
        {
            ulong mask = Vector512.BitwiseOr(
                Vector512.Equals(currentSearchSpace, Vector512.Create((byte)'\n')),
                Vector512.Equals(currentSearchSpace, Vector512.Create((byte)';'))
            ).ExtractMostSignificantBits();
            count += mask.ExtractIndexes(ref Unsafe.Add(ref indexesPlusOneRef, count), index);
            currentSearchSpace = ref Unsafe.Add(ref currentSearchSpace, 1);
            index += Vector512<byte>.Count;
        }
        return int.Min(count, Vector512<int>.Count);
    }

    private static void SerialRemainder(Context context, ref Utf8StringUnsafe dataRef, int dataIndex, ref byte currentSearchSpace, ref byte end)
    {
        if (!Unsafe.IsAddressGreaterThan(ref currentSearchSpace, ref end))
        {
            int lastIndex = 0;
            var remainderSpan = MemoryMarshal.CreateSpan(ref currentSearchSpace, (int)Unsafe.ByteOffset(ref currentSearchSpace, ref end));
            while (true)
            {
                int foundIndex = remainderSpan.IndexOfAny(LineBreakAndComma);
                if (foundIndex == -1)
                    break;

                Unsafe.Add(ref dataRef, dataIndex) = new Utf8StringUnsafe(
                    ref currentSearchSpace,
                    foundIndex);

                if (dataIndex == 1)
                {
                    context.GetOrAdd(ref dataRef)
                        .Add(ParseTemperature(ref Unsafe.Add(ref dataRef, 1)));
                }
                dataIndex ^= 1;
                lastIndex = foundIndex;
                remainderSpan = remainderSpan.Slice(foundIndex + 1);
                currentSearchSpace = ref Unsafe.Add(ref currentSearchSpace, lastIndex + 1);
            }
            if (remainderSpan.Length > 0)
            {
                Unsafe.Add(ref dataRef, dataIndex) = new Utf8StringUnsafe(
                    ref currentSearchSpace,
                    remainderSpan.Length);

                context.GetOrAdd(ref Unsafe.Add(ref dataRef, 0))
                    .Add(ParseTemperature(ref Unsafe.Add(ref dataRef, 1)));
            }
        }
    }

    const long DOT_BITS = 0x10101000;
    const long MAGIC_MULTIPLIER = (100 * 0x1000000 + 10 * 0x10000 + 1);

    /// <summary>
    /// royvanrijn
    /// </summary>
    /// <param name="tempText"></param>
    /// <returns></returns>
    static unsafe int ParseTemperature(ref readonly Utf8StringUnsafe data)
    {
        long word = Unsafe.AsRef<long>(data.Pointer);
        long nword = ~word;
        int decimalSepPos = (int)long.TrailingZeroCount(nword & DOT_BITS);
        long signed = (nword << 59) >> 63;
        long designMask = ~(signed & 0xFF);
        long digits = ((word & designMask) << (28 - decimalSepPos)) & 0x0F000F0F00L;
        long absValue = ((digits * MAGIC_MULTIPLIER) >>> 32) & 0x3FF;
        return (int)((absValue ^ signed) - signed);
        //int currentPosition = 0;
        //int temp;
        //int negative = 1;
        //// Inspired by @yemreinci to unroll this even further
        //if (tempText[currentPosition] == (byte)'-')
        //{
        //    negative = -1;
        //    currentPosition++;
        //}
        //if (tempText[currentPosition + 1] == (byte)'.')
        //    temp = negative * ((tempText[currentPosition] - (byte)'0') * 10 + (tempText[currentPosition + 2] - (byte)'0'));
        //else
        //    temp = negative * ((tempText[currentPosition] - (byte)'0') * 100 + ((tempText[currentPosition + 1] - (byte)'0') * 10 + (tempText[currentPosition + 3] - (byte)'0')));
        //return temp;
    }
}
