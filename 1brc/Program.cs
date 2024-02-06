using System.Buffers;
using System.Collections.Frozen;
using System.Diagnostics;
using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using System.Text;

namespace OneBRC;

class Program
{
    static readonly SearchValues<byte> LineBreakAndComma = SearchValues.Create(";\n"u8);

    static unsafe void Main(string[] args)
    {
        Stopwatch sw = Stopwatch.StartNew();
        string path = args[0].Replace("~", Environment.GetFolderPath(Environment.SpecialFolder.UserProfile));
#if DEBUG
        int parallelism = 1;
#else
        int parallelism = Environment.ProcessorCount;
#endif
        int chunksCount = Environment.ProcessorCount * 2000;
        Debug.WriteLine($"Parallelism: {parallelism}");
        Debug.WriteLine($"Chunks: {chunksCount}");
        Debug.WriteLine($"Vector512.IsHardwareAccelerated: {Vector512.IsHardwareAccelerated}");
        Debug.WriteLine($"Vector256.IsHardwareAccelerated: {Vector256.IsHardwareAccelerated}");
        long length = GetFileLength(path);
        int warmupSize = 2_000_000;
        var consumers = new Task[parallelism];

        Debug.WriteLine($"Starting: {sw.Elapsed}");
        byte[] keysBuffer = GC.AllocateArray<byte>(256 * 1000, true);
        using (var fileHandle = File.OpenHandle(path, FileMode.Open, FileAccess.Read, FileShare.Read, FileOptions.RandomAccess))
        using (var mmf = MemoryMappedFile.CreateFromFile(fileHandle, null, 0, MemoryMappedFileAccess.Read, HandleInheritability.None, true))
        {
            var chunks = CreateChunks(mmf, chunksCount, length);
            Debug.WriteLine($"Start - CreateBaseForContext: {sw.Elapsed}");
            var (smallUniqueKeys, uniqueKeys) = CreateBaseForContext(mmf, chunks, keysBuffer, warmupSize);
            Debug.WriteLine($"End - CreateBaseForContext: {sw.Elapsed}");
            Debug.WriteLine($"Total uniqueKeys: {smallUniqueKeys.Count + uniqueKeys.Count}");
            
            Debug.WriteLine($"Start - Creating Threads: {sw.Elapsed}");
            for (int i = 0; i < consumers.Length; i++)
            {
                consumers[i] = CreateConsumer(
                    new Context(chunks, mmf,
                        smallUniqueKeys.ToFrozenDictionary(kv => kv.Key, kv => new Statistics(kv.Value.Key)),
                        uniqueKeys.ToFrozenDictionary(kv => kv.Key, kv => new Statistics(kv.Value.Key))
                    )
                );
                consumers[i].Start();
            }
            // there is not a lot of allocation from here on, so we can start a no GC region
            GC.TryStartNoGCRegion(1024 * 1024 * 10, true);
            Debug.WriteLine($"End - Creating Threads: {sw.Elapsed}");

            Debug.WriteLine($"Start - OrderedStatistics: {sw.Elapsed}");
            WriteOrderedStatistics(GroupAndAggregateStatistics(consumers, smallUniqueKeys, uniqueKeys));
            Debug.WriteLine($"End - OrderedStatistics: {sw.Elapsed}");

        }
    }

    private static Task CreateConsumer(Context state)
    {
        if (Vector512.IsHardwareAccelerated)
            return new Task(ConsumeVector512, state);
        else if (Vector256.IsHardwareAccelerated)
            return new Task(ConsumeVector256, state);
        else
            return new Task(ConsumeSlow, state);
    }

    private static (Dictionary<int, Statistics>, Dictionary<Utf8StringUnsafe, Statistics>) CreateBaseForContext(MemoryMappedFile mmf, Chunks chunks, byte[] buffer, int warmupSize)
    {
        if (Vector512.IsHardwareAccelerated)
            return CreateBaseForContextVector512(mmf, chunks, buffer, warmupSize);
        else if (Vector256.IsHardwareAccelerated)
            return CreateBaseForContextVector256(mmf, chunks, buffer, warmupSize);
        else
            return CreateBaseForContextSlow(mmf, chunks, buffer, warmupSize);
    }

    private unsafe static (Dictionary<int, Statistics>, Dictionary<Utf8StringUnsafe, Statistics>) CreateBaseForContextSlow(MemoryMappedFile mmf, Chunks chunks, byte[] buffer, int warmupSize)
    {
        var smallResult = new Dictionary<int, Statistics>(16384);
        var result = new Dictionary<Utf8StringUnsafe, Statistics>(16384);
        int bufferPosition = 0;
        int[] indexes = new int[Vector256<int>.Count * sizeof(int)];
        ref int indexesRef = ref indexes[0];
        ref int indexesPlusOneRef = ref indexes[1];
        using (var va = mmf.CreateViewAccessor(0, 0, MemoryMappedFileAccess.Read))
        {
            byte* ptr = (byte*)0;
            va.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);
            ref byte start = ref Unsafe.AsRef<byte>(ptr);
            int consumedSize = 0;
            while (consumedSize < warmupSize
                && chunks.TryGetNext(out var chunk))
            {
                ref byte currentSearchSpace = ref Unsafe.AddByteOffset(ref start, (nint)chunk.Position);
                ref byte end = ref Unsafe.AddByteOffset(ref currentSearchSpace, (nint)chunk.Position);

                SerialRemainder(smallResult, result, buffer, ref bufferPosition, ref currentSearchSpace, ref end);
                consumedSize += chunk.Size;
            }
            va.SafeMemoryMappedViewHandle.ReleasePointer();
        }
        return (smallResult, result);
    }

    private unsafe static (Dictionary<int, Statistics>, Dictionary<Utf8StringUnsafe, Statistics>) CreateBaseForContextVector256(MemoryMappedFile mmf, Chunks chunks, byte[] buffer, int warmupSize)
    {
        var smallResult = new Dictionary<int, Statistics>(16384);
        var result = new Dictionary<Utf8StringUnsafe, Statistics>(16384);
        int bufferPosition = 0;
        int[] indexes = new int[Vector256<int>.Count * sizeof(int) * 4];
        ref int indexesRef = ref indexes[0];
        ref int indexesPlusOneRef = ref indexes[1];
        using (var va = mmf.CreateViewAccessor(0, 0, MemoryMappedFileAccess.Read))
        {
            byte* ptr = (byte*)0;
            va.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);
            ref byte start = ref Unsafe.AsRef<byte>(ptr);
            int consumedSize = 0;
            while (consumedSize < warmupSize
                && chunks.TryGetNext(out var chunk))
            {
                ref byte currentSearchSpace = ref Unsafe.AddByteOffset(ref start, (nint)chunk.Position);
                ref byte end = ref Unsafe.AddByteOffset(ref currentSearchSpace, (nint)chunk.Size);
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

                    var (first, second) = GetOrAddExtractStatistics(smallResult, result, buffer, ref bufferPosition, lowAddresses, lowSizes);
                    var (third, fourth) = GetOrAddExtractStatistics(smallResult, result, buffer, ref bufferPosition, highAddresses, highSizes);

                    first.Add(fixedPoints[0]);
                    second.Add(fixedPoints[8]);
                    third.Add(fixedPoints[4]);
                    fourth.Add(fixedPoints[12]);

                    currentSearchSpace = ref Unsafe.Add(ref currentSearchSpace, lastIndex);
                }
                SerialRemainder(smallResult, result, buffer, ref bufferPosition, ref currentSearchSpace, ref end);
                consumedSize += chunk.Size;
            }
            va.SafeMemoryMappedViewHandle.ReleasePointer();
        }
        return (smallResult, result);
    }

    private static unsafe (Statistics, Statistics) GetOrAddExtractStatistics(Dictionary<int, Statistics> smallResult, Dictionary<Utf8StringUnsafe, Statistics> result, byte[] buffer, ref int bufferPosition, Vector256<long> addresses, Vector256<long> sizes)
    {
        var addressesAndSizes = Avx2.UnpackLow(addresses, sizes);
        ref var stringUnsafe = ref Unsafe.As<Vector256<long>, Utf8StringUnsafe>(ref addressesAndSizes);

        return (GetOrAdd(smallResult, result, buffer, ref bufferPosition, ref stringUnsafe), GetOrAdd(smallResult, result, buffer, ref bufferPosition, ref Unsafe.Add(ref stringUnsafe, 1)));
    }


    private unsafe static (Dictionary<int, Statistics>, Dictionary<Utf8StringUnsafe, Statistics>) CreateBaseForContextVector512(MemoryMappedFile mmf, Chunks chunks, byte[] buffer, int warmupSize)
    {
        var smallResult = new Dictionary<int, Statistics>(16384);
        var result = new Dictionary<Utf8StringUnsafe, Statistics>(16384);
        int bufferPosition = 0;
        int[] indexes = new int[Vector512<int>.Count * sizeof(int)];
        ref int indexesRef = ref indexes[0];
        ref int indexesPlusOneRef = ref indexes[1];
        using (var va = mmf.CreateViewAccessor(0, 0, MemoryMappedFileAccess.Read))
        {
            byte* ptr = (byte*)0;
            va.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);
            ref byte start = ref Unsafe.AsRef<byte>(ptr);
            int consumedSize = 0;
            while (consumedSize < warmupSize
                && chunks.TryGetNext(out var chunk))
            {
                ref byte currentSearchSpace = ref Unsafe.AddByteOffset(ref start, (nint)chunk.Position);
                ref byte end = ref Unsafe.AddByteOffset(ref currentSearchSpace, (nint)chunk.Size);
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
                    GetOrAddUnpackedPartsExtractStatistics(smallResult, result, buffer, ref bufferPosition, ref lowAddress, ref lowSizes);

                    Vector512<long> highAddress = highAddressOffset + currentSearchSpaceAddressVector;
                    GetOrAddUnpackedPartsExtractStatistics(smallResult, result, buffer, ref bufferPosition, ref highAddress, ref highSizes);

                    uint lastIndex = (uint)(Unsafe.Add(ref Unsafe.As<Vector512<int>, int>(ref addressesVectorRef), count - 1) + Unsafe.Add(ref Unsafe.As<Vector512<int>, int>(ref sizesVectorRef), count - 1) + 1);
                    currentSearchSpace = ref Unsafe.Add(ref currentSearchSpace, lastIndex);
                }
                SerialRemainder(smallResult, result, buffer, ref bufferPosition, ref currentSearchSpace, ref end);
                consumedSize += chunk.Size;
            }
            va.SafeMemoryMappedViewHandle.ReleasePointer();
        }
        return (smallResult, result);
    }

    private static void SerialRemainder(Dictionary<int, Statistics> smallResult, Dictionary<Utf8StringUnsafe, Statistics> result, byte[] buffer, ref int bufferPosition, ref byte currentSearchSpace, ref byte end)
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

                 GetOrAdd(smallResult, result, buffer, ref bufferPosition, ref city)
                    .Add(ParseTemperature(ref temperature));

                lastIndex = lineBreakIndex + 1 + commaIndex + 1;
                remainderSpan = remainderSpan.Slice(lastIndex);
                currentSearchSpace = ref Unsafe.Add(ref currentSearchSpace, lastIndex);
            }
        }
    }

    private static unsafe void GetOrAddUnpackedPartsExtractStatistics(Dictionary<int, Statistics> smallResult, Dictionary<Utf8StringUnsafe, Statistics> result, byte[] buffer, ref int bufferPosition, ref readonly Vector512<long> addresses, ref readonly Vector512<long> sizes)
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

        GetOrAdd(smallResult, result, buffer, ref bufferPosition, ref lowStringUnsafe)
            .Add(fixedPoints[0]);
        GetOrAdd(smallResult, result, buffer, ref bufferPosition, ref Unsafe.Add(ref lowStringUnsafe, 1))
            .Add(fixedPoints[4]);
        GetOrAdd(smallResult, result, buffer, ref bufferPosition, ref Unsafe.Add(ref lowStringUnsafe, 2))
            .Add(fixedPoints[8]);
        GetOrAdd(smallResult, result, buffer, ref bufferPosition, ref Unsafe.Add(ref lowStringUnsafe, 3))
            .Add(fixedPoints[12]);
    }

    private static unsafe Statistics GetOrAdd(Dictionary<int, Statistics> smallResult, Dictionary<Utf8StringUnsafe, Statistics> result, byte[] buffer, ref int bufferPosition, ref readonly Utf8StringUnsafe key)
    {
        Statistics? statistics;
        switch (key.Length)
        {
            case 1:
            case 2:
            case 3:
                {
                    int smallKey = Unsafe.ReadUnaligned<int>(ref key.PointerRef) & (1 << (key.Length * 8)) - 1;
                    if (!smallResult.TryGetValue(smallKey, out statistics))
                    {
                        statistics = new Statistics(key.ToString());
                        smallResult.Add(smallKey, statistics);
                    }
                    break;
                }
            case 4:
                {
                    int smallKey = Unsafe.ReadUnaligned<int>(ref key.PointerRef);
                    if (!smallResult.TryGetValue(smallKey, out statistics))
                    {
                        statistics = new Statistics(key.ToString());
                        smallResult.Add(smallKey, statistics);
                    }
                    break;
                }
            default:
                if (!result.TryGetValue(key, out statistics))
                {

                    ref var bufferRef = ref MemoryMarshal.GetArrayDataReference(buffer);
                    ref var destinationRef = ref Unsafe.Add(ref bufferRef, bufferPosition);
                    Unsafe.CopyBlockUnaligned(ref destinationRef, ref key.PointerRef, (uint)key.Length);
                    var keyCopy = new Utf8StringUnsafe(ref destinationRef, key.Length);
                    statistics = new Statistics(keyCopy.ToString());
                    result.Add(keyCopy, statistics);
                    bufferPosition += key.Length;
                }
                break;
        }

        return statistics;
    }

    private static unsafe Chunks CreateChunks(MemoryMappedFile mmf, int chunks, long length)
    {
        var result = new List<Chunk>(chunks + 2);
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

                    result.Add(new Chunk(position, lastIndexOfLineBreak + 1));
                    position += (long)lastIndexOfLineBreak + 1;
                }
            }
            va.SafeMemoryMappedViewHandle.ReleasePointer();
        }
        return new Chunks(result.ToArray());
    }

    private static long GetFileLength(string path)
    {
        using (var file = File.OpenRead(path))
            return file.Length;
    }

    private static void WriteOrderedStatistics(SortedSet<Statistics> final)
    {
        StringBuilder sb = new StringBuilder(final.Count * 256);
        bool first = true;
        sb.Append('{');
        foreach (var statistics in final)
        {
            if (first)
                first = false;
            else
                sb.Append(", ");

            sb.Append($"{statistics.Key}={(statistics.Min / 10f):0.0}/{(float)(statistics.Sum / 10f) / statistics.Count:0.0}/{(statistics.Max / 10f):0.0}");
        }
        sb.Append('}');
        Console.WriteLine(sb.ToString());
        Debug.Assert(final.Sum(f => f.Count) == 1_000_000_000);
    }

    private unsafe static SortedSet<Statistics> GroupAndAggregateStatistics(Task[] consumers, IDictionary<int, Statistics> warmupSmallDictionary, IDictionary<Utf8StringUnsafe, Statistics> warmupDictionary)
    {
        var list = new SortedSet<Statistics>();
        var final = new Dictionary<Utf8StringUnsafe, Statistics>(32768);
        var smallFinal = new Dictionary<int, Statistics>(32768);
        Merge(warmupSmallDictionary, warmupDictionary, smallFinal, final, list);
        var consumersList = consumers.ToList();
        while (consumersList.Count > 0)
        {
            var finalized = Task.WhenAny(consumersList).Result;
            if (finalized.AsyncState is Context context)
                Merge(context.SmallKeys, context.Keys, smallFinal, final, list);
            consumersList.Remove(finalized);
        }
        return list;
    }

    private static unsafe void Merge(IDictionary<int, Statistics> warmupSmallDictionary, IDictionary<Utf8StringUnsafe, Statistics> warmupDictionary, IDictionary<int, Statistics> smallFinal, Dictionary<Utf8StringUnsafe, Statistics> final, SortedSet<Statistics> list)
    {
        foreach (var data in warmupSmallDictionary)
        {
            if (!smallFinal.TryGetValue(data.Key, out var stats))
            {
                stats = data.Value;
                smallFinal.Add(data.Key, stats);
                list.Add(stats);
            }
            else
            {
                stats.Count += data.Value.Count;
                stats.Sum += data.Value.Sum;
                stats.Min = short.Min(stats.Min, data.Value.Min);
                stats.Max = short.Max(stats.Max, data.Value.Max);
            }
        }

        foreach (var data in warmupDictionary)
        {
            if (!final.TryGetValue(data.Key, out var stats))
            {
                stats = data.Value;
                final.Add(data.Key, stats);
                list.Add(stats);
            }
            else
            {
                stats.Count += data.Value.Count;
                stats.Sum += data.Value.Sum;
                stats.Min = short.Min(stats.Min, data.Value.Min);
                stats.Max = short.Max(stats.Max, data.Value.Max);
            }
        }
    }

    private unsafe static void ConsumeSlow(object? obj)
    {
        ArgumentNullException.ThrowIfNull(obj);
        Context context = (Context)obj;

        using (var va = context.MappedFile.CreateViewAccessor(0, 0, MemoryMappedFileAccess.Read))
        {
            byte* ptr = (byte*)0;
            va.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);
            while (context.Chunks.TryGetNext(out var chunk))
                ConsumeSlow(context, ptr + chunk.Position, chunk.Size);
            va.SafeMemoryMappedViewHandle.ReleasePointer();
        }
    }
    private unsafe static void ConsumeVector256(object? obj)
    {
        ArgumentNullException.ThrowIfNull(obj);
        Context context = (Context)obj;

        int[] indexes = new int[Vector256<int>.Count * sizeof(int) * 4];
        ref int indexesRef = ref indexes[0];
        ref int indexesPlusOneRef = ref indexes[1];
        using (var va = context.MappedFile.CreateViewAccessor(0, 0, MemoryMappedFileAccess.Read))
        {
            byte* ptr = (byte*)0;
            va.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);
            ref byte start = ref Unsafe.AsRef<byte>(ptr);
            while (context.Chunks.TryGetNext(out var chunk))
                ConsumeWithVector256(context, ref indexesRef, ref indexesPlusOneRef, ref Unsafe.AddByteOffset(ref start, (nint)chunk.Position), chunk.Size);
            va.SafeMemoryMappedViewHandle.ReleasePointer();
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
            while (context.Chunks.TryGetNext(out var chunk))
                ConsumeWithVector512(context, ref indexesRef, ref indexesPlusOneRef, ref Unsafe.AddByteOffset(ref start, (nint)chunk.Position), chunk.Size);
            va.SafeMemoryMappedViewHandle.ReleasePointer();
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

            uint lastIndex = (uint)(addressesVectorRef[7] + sizesVectorRef[7] + 1);
            
            var (lowAddressesOffset, highAddressesOffset) = Vector256.Widen(addressesVectorRef);
            Vector256<short> fixedPoints = Avx2.GatherVector256(
                (long*)Unsafe.AsPointer(ref currentSearchSpace),
                Avx2.UnpackHigh(lowAddressesOffset, highAddressesOffset), 1
            ).ParseQuadFixedPoint();

            var currentSearchSpaceAddressVector = Vector256.Create((long)(nint)Unsafe.AsPointer(ref currentSearchSpace));

            var lowAddresses = lowAddressesOffset + currentSearchSpaceAddressVector;
            var highAddresses = highAddressesOffset + currentSearchSpaceAddressVector;

            var (lowSizes, highSizes) = Vector256.Widen(sizesVectorRef);
            var (first, second) = ExtractStatistics(context, lowAddresses, lowSizes);
            var (third, fourth) = ExtractStatistics(context, highAddresses, highSizes);

            first.Add(fixedPoints[0]);
            second.Add(fixedPoints[8]);
            third.Add(fixedPoints[4]);
            fourth.Add(fixedPoints[12]);

            currentSearchSpace = ref Unsafe.Add(ref currentSearchSpace, lastIndex);
        }
        SerialRemainder(context, ref currentSearchSpace, ref end);
    }

    private static unsafe (Statistics, Statistics) ExtractStatistics(Context context, Vector256<long> addresses, Vector256<long> sizes)
    {
        var addressesAndSizes = Avx2.UnpackLow(addresses, sizes);
        ref var stringUnsafe = ref Unsafe.As<Vector256<long>, Utf8StringUnsafe>(ref addressesAndSizes);

        return (context.Get(ref stringUnsafe), context.Get(ref Unsafe.Add(ref stringUnsafe, 1)));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static uint ExtractMaskEqualityToLineBreakOrComma(in Vector256<byte> currentSearchSpace)
    {
        return Vector256.BitwiseOr(
            Vector256.Equals(currentSearchSpace, Vector256.Create((byte)'\n')),
            Vector256.Equals(currentSearchSpace, Vector256.Create((byte)';'))
        ).ExtractMostSignificantBits();
    }
    private static int ExtractIndexesVector256(ref byte start, ref byte end, ref int indexesPlusOneRef)
    {
        ref var currentSearchSpace = ref Unsafe.As<byte, Vector256<byte>>(ref start);
        ref var oneVectorAwayFromEnd = ref Unsafe.As<byte, Vector256<byte>>(ref end);
        ref var twoVectorAwayFromEnd = ref Unsafe.Subtract(ref oneVectorAwayFromEnd, 1);
        int index = 0;
        int count = 0;
        while(!Unsafe.IsAddressGreaterThan(ref currentSearchSpace, ref twoVectorAwayFromEnd)
            && count < Vector256<int>.Count)
        {
            uint mask1 = ExtractMaskEqualityToLineBreakOrComma(in currentSearchSpace);
            uint mask2 = ExtractMaskEqualityToLineBreakOrComma(in Unsafe.Add(ref currentSearchSpace, 1));
    
            count += mask1.ExtractIndexes(ref Unsafe.Add(ref indexesPlusOneRef, count), index);
            count += mask2.ExtractIndexes(ref Unsafe.Add(ref indexesPlusOneRef, count), index + Vector256<byte>.Count);
            currentSearchSpace = ref Unsafe.Add(ref currentSearchSpace, 2);
            index += Vector256<byte>.Count + Vector256<byte>.Count;
        }
        while (!Unsafe.IsAddressGreaterThan(ref currentSearchSpace, ref oneVectorAwayFromEnd)
            && count < Vector256<int>.Count)
        {
            count += ExtractMaskEqualityToLineBreakOrComma(in currentSearchSpace)
                .ExtractIndexes(ref Unsafe.Add(ref indexesPlusOneRef, count), index);
            currentSearchSpace = ref Unsafe.Add(ref currentSearchSpace, 1);
            index += Vector256<byte>.Count;
        }

        return int.Min(count, Vector256<int>.Count);
    }
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ulong ExtractMaskEqualityToLineBreakOrComma(in Vector512<byte> currentSearchSpace)
    {
        return Vector512.BitwiseOr(
            Vector512.Equals(currentSearchSpace, Vector512.Create((byte)'\n')),
            Vector512.Equals(currentSearchSpace, Vector512.Create((byte)';'))
        ).ExtractMostSignificantBits();
    }
    private static int ExtractIndexesVector512(ref byte start, ref byte end, ref int indexesPlusOneRef)
    {
        ref var currentSearchSpace = ref Unsafe.As<byte, Vector512<byte>>(ref start);
        ref var oneVectorAwayFromEnd = ref Unsafe.As<byte, Vector512<byte>>(ref end);
        ref var twoVectorAwayFromEnd = ref Unsafe.Subtract(ref oneVectorAwayFromEnd, 1);
        int index = 0;
        int count = 0;
        while (!Unsafe.IsAddressGreaterThan(ref currentSearchSpace, ref twoVectorAwayFromEnd)
            && count < Vector512<int>.Count)
        {
            ulong mask1 = ExtractMaskEqualityToLineBreakOrComma(in currentSearchSpace);
            ulong mask2 = ExtractMaskEqualityToLineBreakOrComma(in Unsafe.Add(ref currentSearchSpace, 1));

            count += mask1.ExtractIndexes(ref Unsafe.Add(ref indexesPlusOneRef, count), index);
            count += mask2.ExtractIndexes(ref Unsafe.Add(ref indexesPlusOneRef, count), index + Vector512<byte>.Count);
            currentSearchSpace = ref Unsafe.Add(ref currentSearchSpace, 2);
            index += Vector512<byte>.Count + Vector512<byte>.Count;
        }
        while (!Unsafe.IsAddressGreaterThan(ref currentSearchSpace, ref oneVectorAwayFromEnd)
            && count < Vector512<int>.Count)
        {
            ulong mask = ExtractMaskEqualityToLineBreakOrComma(in currentSearchSpace);
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
    /// quan ahn
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
