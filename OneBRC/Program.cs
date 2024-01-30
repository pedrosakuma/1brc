using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Frozen;
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

        byte[] keysBuffer = new byte[256 * 1000];
        using (var fileHandle = File.OpenHandle(path, FileMode.Open, FileAccess.Read, FileShare.Read, FileOptions.RandomAccess))
        using (var mmf = MemoryMappedFile.CreateFromFile(fileHandle, null, 0, MemoryMappedFileAccess.Read, HandleInheritability.None, true))
        {
            ConcurrentQueue<Chunk> chunkQueue = new ConcurrentQueue<Chunk>();
            new Thread(() =>
            {
                CreateChunks(mmf, chunkQueue, chunks, length);
            }).Start();
            var uniqueKeys = CreateBaseForContext(mmf, keysBuffer, 2_000_000);
            for (int i = 0; i < parallelism; i++)
            {
                int index = i;
                contexts[i] = new Context(chunkQueue, mmf, uniqueKeys.ToFrozenDictionary(kv => kv, kv => new Statistics()));
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

    private static unsafe HashSet<Utf8StringUnsafe> CreateBaseForContext(MemoryMappedFile mmf, byte[] buffer, long totalSize)
    {
        HashSet<Utf8StringUnsafe> uniqueKeys = new HashSet<Utf8StringUnsafe>(262144);
        int bufferPosition = 0;
        int[] indexes = new int[Vector256<int>.Count * sizeof(int)];
        ref int indexesRef = ref indexes[0];
        ref int indexesPlusOneRef = ref indexes[1];
        using (var va = mmf.CreateViewAccessor(0, 0, MemoryMappedFileAccess.Read))
        {
            byte* ptr = (byte*)0;
            va.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);
            ref byte start = ref Unsafe.AsRef<byte>(ptr);

            ref byte currentSearchSpace = ref start;
            ref byte end = ref Unsafe.AddByteOffset(ref start, (nint)totalSize);
            ref byte oneVectorAwayFromEnd = ref Unsafe.Subtract(ref end, Vector256<byte>.Count);

            int count;
            while ((count = ExtractIndexesVector256(ref currentSearchSpace, ref oneVectorAwayFromEnd, ref indexesPlusOneRef)) == Vector256<int>.Count)
            {
                var add = Vector256.Create(0, 1, 1, 1, 1, 1, 1, 1);
                var indexesVectorRef = Unsafe.As<int, Vector256<int>>(ref indexesRef);
                var addressesVectorRef = indexesVectorRef + add;
                var sizesVectorRef = Unsafe.As<int, Vector256<int>>(ref indexesPlusOneRef) - indexesVectorRef - add;

                var (lowAddressesOffset, highAddressesOffset) = Vector256.Widen(addressesVectorRef);
                var currentSearchSpaceAddressVector = Vector256.Create((long)(nint)Unsafe.AsPointer(ref currentSearchSpace));

                uint lastIndex = (uint)(addressesVectorRef[7] + sizesVectorRef[7] + 1);

                var lowAddresses = lowAddressesOffset + currentSearchSpaceAddressVector;
                var highAddresses = highAddressesOffset + currentSearchSpaceAddressVector;

                Vector256<short> fixedPoints = Avx2.GatherVector256(
                    (long*)0,
                    Avx2.UnpackHigh(lowAddresses, highAddresses), 1
                ).ParseQuadFixedPoint();

                var (lowSizes, highSizes) = Vector256.Widen(sizesVectorRef);

                var lowAddressesAndSizes = Avx2.UnpackLow(lowAddresses, lowSizes);
                ref var lowStringUnsafe = ref Unsafe.As<Vector256<long>, Utf8StringUnsafe>(ref lowAddressesAndSizes);
                Add(uniqueKeys, buffer, ref bufferPosition, lowStringUnsafe);
                uniqueKeys.Add(Unsafe.Add(ref lowStringUnsafe, 1));

                var highAddressesAndSizes = Avx2.UnpackLow(highAddresses, highSizes);
                ref var highStringUnsafe = ref Unsafe.As<Vector256<long>, Utf8StringUnsafe>(ref highAddressesAndSizes);

                uniqueKeys.Add(highStringUnsafe);
                uniqueKeys.Add(Unsafe.Add(ref highStringUnsafe, 1));

                currentSearchSpace = ref Unsafe.Add(ref currentSearchSpace, lastIndex);
            }
        }
        return uniqueKeys;
    }

    private static unsafe void Add(HashSet<Utf8StringUnsafe> uniqueKeys, byte[] buffer, ref int bufferPosition, Utf8StringUnsafe key)
    {
        if (!uniqueKeys.Contains(key))
        {
            ref var bufferRef = ref MemoryMarshal.GetArrayDataReference(buffer);
            ref var destinationRef = ref Unsafe.Add(ref bufferRef, bufferPosition);
            Unsafe.CopyBlockUnaligned(ref destinationRef, ref key.PointerRef, (uint)key.Length);
            uniqueKeys.Add(new Utf8StringUnsafe(ref destinationRef, key.Length));
            bufferPosition += key.Length;
        }
    }

    private static unsafe void CreateChunks(MemoryMappedFile mmf, ConcurrentQueue<Chunk> chunkQueue, int chunks, long length)
    {
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

                    chunkQueue.Enqueue(new Chunk(position, lastIndexOfLineBreak));
                    position += (long)lastIndexOfLineBreak + 1;
                }
            }
        }
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

    private unsafe static Dictionary<string, Statistics> GroupAndAggregateStatistics(Context[] contexts)
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
                stats.Min = short.Min(stats.Min, data.Value.Min);
                stats.Max = short.Max(stats.Max, data.Value.Max);
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

        int[] indexes = new int[sizeof(int) * 8];
        ref int indexesRef = ref indexes[0];
        ref int indexesPlusOneRef = ref indexes[1];
        using (var va = context.MappedFile.CreateViewAccessor(0, 0, MemoryMappedFileAccess.Read))
        {
            byte* ptr = (byte*)0;
            va.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);
            ref byte start = ref Unsafe.AsRef<byte>(ptr);
            while (context.ChunkQueue.TryDequeue(out var chunk))
                ConsumeWithVector256(context, ref indexesRef, ref indexesPlusOneRef, ref Unsafe.AddByteOffset(ref start, (nint)chunk.Position), chunk.Size);
        }
    }
    private unsafe static void ConsumeVector512(object? obj)
    {
        ArgumentNullException.ThrowIfNull(obj);
        Context context = (Context)obj;

        int[] indexes = new int[Vector512<int>.Count * sizeof(int)];
        ref int indexesRef = ref indexes[0];
        ref int indexesPlusOneRef = ref indexes[1];
        using (var va = context.MappedFile.CreateViewAccessor(0, 0, MemoryMappedFileAccess.Read))
        {
            byte* ptr = (byte*)0;
            va.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);
            ref byte start = ref Unsafe.AsRef<byte>(ptr);
            while (context.ChunkQueue.TryDequeue(out var chunk))
                ConsumeWithVector512(context, ref indexesRef, ref indexesPlusOneRef, ref Unsafe.AddByteOffset(ref start, (nint)chunk.Position), chunk.Size);
        }
    }

    private static unsafe void ConsumeSlow(Context context, byte* ptr, int size)
    {
        Utf8StringUnsafe[] data = new Utf8StringUnsafe[16];
        ref var dataRef = ref MemoryMarshal.GetArrayDataReference(data);

        ref byte searchSpace = ref Unsafe.AsRef<byte>(ptr);

        ref byte currentSearchSpace = ref searchSpace;
        ref byte end = ref Unsafe.Add(ref searchSpace, size);
        SerialRemainder(context, ref currentSearchSpace, ref end);
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private unsafe static void ConsumeWithVector512(Context context, ref int indexesRef, ref int indexesPlusOneRef, ref byte searchSpace, int size)
    {
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
        SerialRemainder(context, ref currentSearchSpace, ref end);
    }

    private static unsafe void GetOrAddUnpackedParts(Context context, ref readonly Vector512<long> addresses, ref readonly Vector512<long> sizes)
    {
        var lowAddressesAndSizes = Avx512F.UnpackLow(addresses, sizes);
        var highAddressesAndSizes = Avx512F.UnpackHigh(addresses, sizes);

        ref var lowStringUnsafe = ref Unsafe.As<Vector512<long>, Utf8StringUnsafe>(ref lowAddressesAndSizes);
        ref var highStringUnsafe = ref Unsafe.As<Vector512<long>, Utf8StringUnsafe>(ref highAddressesAndSizes);

        Vector256<short> fixedPoints = Vector256.Create(
            *(long*)(nint)addresses[1],
            *(long*)(nint)addresses[3],
            *(long*)(nint)addresses[5],
            *(long*)(nint)addresses[7]
        ).ParseQuadFixedPoint();

        context.Get(ref lowStringUnsafe)
            .Add(fixedPoints[0]);
        context.Get(ref Unsafe.Add(ref lowStringUnsafe, 1))
            .Add(fixedPoints[4]);
        context.Get(ref Unsafe.Add(ref lowStringUnsafe, 2))
            .Add(fixedPoints[8]);
        context.Get(ref Unsafe.Add(ref lowStringUnsafe, 3))
            .Add(fixedPoints[12]);
    }

    private static unsafe void ConsumeWithVector256(Context context, ref int indexesRef, ref int indexesPlusOneRef, ref byte searchSpace, int size)
    {
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
            var currentSearchSpaceAddressVector = Vector256.Create((long)(nint)Unsafe.AsPointer(ref currentSearchSpace));
            
            uint lastIndex = (uint)(addressesVectorRef[7] + sizesVectorRef[7] + 1);

            var lowAddresses = lowAddressesOffset + currentSearchSpaceAddressVector;
            var highAddresses = highAddressesOffset + currentSearchSpaceAddressVector;

            Vector256<short> fixedPoints = Avx2.GatherVector256(
                (long*)0,
                Avx2.UnpackHigh(lowAddresses, highAddresses), 1
            ).ParseQuadFixedPoint();

            var (lowSizes, highSizes) = Vector256.Widen(sizesVectorRef);
            var (first, second) = ExtractStatistics(context, lowSizes, lowAddresses);
            var (third, fourth) = ExtractStatistics(context, highSizes, highAddresses);

            first.Add(fixedPoints[0]);
            second.Add(fixedPoints[4]);
            third.Add(fixedPoints[8]);
            fourth.Add(fixedPoints[12]);

            currentSearchSpace = ref Unsafe.Add(ref currentSearchSpace, lastIndex);
        }
        SerialRemainder(context, ref currentSearchSpace, ref end);
    }

    private static unsafe (Statistics, Statistics) ExtractStatistics(Context context, Vector256<long> sizes, Vector256<long> addresses)
    {
        var addressesAndSizes = Avx2.UnpackLow(addresses, sizes);
        ref var stringUnsafe = ref Unsafe.As<Vector256<long>, Utf8StringUnsafe>(ref addressesAndSizes);

        return (context.Get(ref stringUnsafe), context.Get(ref Unsafe.Add(ref stringUnsafe, 1)));
    }

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

    private static void SerialRemainder(Context context, ref byte currentSearchSpace, ref byte end)
    {
        if (!Unsafe.IsAddressGreaterThan(ref currentSearchSpace, ref end))
        {
            int lastIndex = 0;
            var remainderSpan = MemoryMarshal.CreateSpan(ref currentSearchSpace, (int)Unsafe.ByteOffset(ref currentSearchSpace, ref end));
            while (true)
            {
                int commaIndex = remainderSpan.IndexOf((byte)';');
                if (commaIndex == -1)
                    break;
                int lineBreakIndex = remainderSpan.Slice(commaIndex + 1).IndexOf((byte)'\n');
                if (lineBreakIndex == -1)
                    break;

                var city = new Utf8StringUnsafe(
                    ref currentSearchSpace,
                    commaIndex);
                var temperature = new Utf8StringUnsafe(
                    ref Unsafe.Add(ref currentSearchSpace, commaIndex + 1),
                    lineBreakIndex);

                context.Get(ref city)
                    .Add(ParseTemperature(ref temperature));

                lastIndex = lineBreakIndex + 1 + commaIndex + 1;
                remainderSpan = remainderSpan.Slice(lastIndex);
                currentSearchSpace = ref Unsafe.Add(ref currentSearchSpace, lastIndex);
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
    static unsafe short ParseTemperature(ref readonly Utf8StringUnsafe data)
    {
        long word = Unsafe.As<byte, long>(ref data.PointerRef);
        long nword = ~word;
        int decimalSepPos = (int)long.TrailingZeroCount(nword & DOT_BITS);
        long signed = (nword << 59) >> 63;
        long designMask = ~(signed & 0xFF);
        long digits = ((word & designMask) << (28 - decimalSepPos)) & 0x0F000F0F00L;
        long absValue = ((digits * MAGIC_MULTIPLIER) >>> 32) & 0x3FF;
        return (short)((absValue ^ signed) - signed);
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
