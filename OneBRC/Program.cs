using System;
using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;

namespace OneBRC;

class Program
{
    static void Main(string[] args)
    {
        var sw = Stopwatch.StartNew();
        ProcessQueues processQueues = new ProcessQueues(args[0], 16);

        var producer = new Thread(Produce);
        producer.Start(processQueues);
        
        var consumers = Enumerable.Range(0, processQueues.Parallelism)
            .Select(_ => new Thread(Consume)).ToArray();

        foreach (var consumer in consumers)
            consumer.Start(processQueues);

        producer.Join();
        foreach (var consumer in consumers)
            consumer.Join();

        WriteOrderedStatistics(processQueues.Contexts.First().ordered, GroupAndAggregateStatistics(processQueues));
        Console.WriteLine(sw.Elapsed);
    }

    private static void WriteOrderedStatistics(List<string> ordered, Dictionary<string, Statistics> final)
    {
        bool first = true;
        Console.Write("{");
        foreach (var item in ordered)
        {
            Statistics statistics = final[item];
            if (first)
                first = false;
            else
                Console.Write(", ");

            Console.Write($"{item}={(statistics.Min / 10f).ToString("0.0")}/{(float)(statistics.Sum / 10f) / statistics.Count:0.0}/{(statistics.Max / 10f).ToString("0.0")}");
        }
        Console.WriteLine("}");
    }

    private static Dictionary<string, Statistics> GroupAndAggregateStatistics(ProcessQueues processQueues)
    {
        Dictionary<string, Statistics> final = new Dictionary<string, Statistics>();
        foreach (var context in processQueues.Contexts)
        {
            foreach (var data in context.data)
            {
                if (!final.TryGetValue(data.Key, out var stats))
                {
                    stats = new Statistics();
                    final.Add(data.Key, stats);
                }
                stats.Count += data.Value.Count;
                stats.Sum += data.Value.Sum;
                stats.Min = int.Min(stats.Min, data.Value.Min);
                stats.Max = int.Max(stats.Max, data.Value.Max);
            }
        }

        return final;
    }

    private static void Consume(object? data)
    {
        ArgumentNullException.ThrowIfNull(data);
        var p = (ProcessQueues)data;
        while (p.ProcessingQueue.TryTake(out var context, -1))
        {
            var span = context.BlockBuffer.AsSpan(0, context.BlockBufferSize);
            context.LinesCount = GetLines(context.Indexes, context.Lengths, span);

            for (int i = 0; i < context.LinesCount; i++)
            {
                ReadOnlySpan<byte> line = span
                    .Slice(context.Indexes[i], context.Lengths[i]);
                ProcessMessage(context, line);
            }
            if (context != null && !p.FreeQueue.IsAddingCompleted)
                p.FreeQueue.Add(context);
        }
    }

    private static void Produce(object? data)
    {
        ArgumentNullException.ThrowIfNull(data);
        var p = (ProcessQueues)data;
        using (var measurements = File.OpenRead(p.Path))
        {
            int readOffset = 0;
            byte[] remainder = new byte[256];
            while (p.FreeQueue.TryTake(out var context, -1))
            {
                var blockBuffer = context.BlockBuffer;

                remainder.AsSpan(0, readOffset).CopyTo(blockBuffer);
                int size = measurements.Read(blockBuffer, readOffset, blockBuffer.Length - readOffset);
                if (size == 0)
                {
                    p.ProcessingQueue.CompleteAdding();
                    p.FreeQueue.CompleteAdding();
                    break;
                }
                context.BlockBufferSize = size + readOffset;

                var span = blockBuffer.AsSpan(0, context.BlockBufferSize);

                int lastIndexOfLineBreak = span.LastIndexOf((byte)'\n');

                p.ProcessingQueue.Add(context);

                readOffset = context.BlockBufferSize - (lastIndexOfLineBreak + 1);
                span.Slice(lastIndexOfLineBreak + 1).CopyTo(remainder);
            }
        }
    }

    static int GetLines(int[] indexes, int[] lengths, Span<byte> source)
    {
        if (Avx2.IsSupported)
            return GetLinesAvx2(indexes, lengths, source);
        else
            return GetLinesScalar(indexes, lengths, source);
        
    }

    private static int GetLinesScalar(int[] indexes, int[] lengths, Span<byte> source)
    {
        int lineCount = 0;
        int offset = 0;
        while (true)
        {
            var lineBreakIndex = source.IndexOf((byte)'\n');
            if (lineBreakIndex == -1)
                break;

            var currentCount = lineCount++;
            indexes[currentCount] = offset;
            lengths[currentCount] = lineBreakIndex;

            source = source.Slice(lineBreakIndex + 1);
            offset += lineBreakIndex + 1;
        }
        return lineCount;
    }

    private static int GetLinesAvx2(int[] indexes, int[] lengths, Span<byte> source)
    {
        int resultIndex = 0;

        ref byte searchSpace = ref MemoryMarshal.GetReference(source);

        ref byte currentSearchSpace = ref searchSpace;
        ref byte oneVectorAwayFromEnd = ref Unsafe.Add(ref searchSpace, (uint)(source.Length - Vector256<byte>.Count));

        ref int indexesRef = ref MemoryMarshal.GetReference(indexes.AsSpan());
        ref int lengthsRef = ref MemoryMarshal.GetReference(lengths.AsSpan());
        int index = 0;
        int offset = 0;
        Vector256<byte> lineBreak = Vector256.Create((byte)'\n');

        do
        {
            int mask = Avx2.MoveMask(
                Avx2.CompareEqual(
                    Vector256.LoadUnsafe(ref currentSearchSpace),
                    lineBreak
                )
            );
            int tzcnt = int.TrailingZeroCount(mask);
            while (tzcnt != 32)
            {
                var lineBreakIndex = tzcnt + 1 + index;
                var currentIndex = resultIndex++;
                Unsafe.Add(ref indexesRef, currentIndex) = offset;
                Unsafe.Add(ref lengthsRef, currentIndex) = lineBreakIndex - offset - 1;

                mask ^= 1 << tzcnt;
                tzcnt = int.TrailingZeroCount(mask);
                if (resultIndex == indexes.Length)
                    return resultIndex;
                offset = lineBreakIndex;
            }

            index += Vector256<byte>.Count;
            currentSearchSpace = ref Unsafe.Add(ref currentSearchSpace, Vector256<byte>.Count);
        }
        while (!Unsafe.IsAddressGreaterThan(ref currentSearchSpace, ref oneVectorAwayFromEnd));
        return resultIndex;
    }

    static void ProcessMessage(Context context, ReadOnlySpan<byte> span)
    {
        var separator = span.IndexOf((byte)';');
        context.GetOrAdd(span.Slice(0, separator)).Add(
            ParseTemperature(
                span.Slice(separator + 1, span.Length - (separator + 1) - 1)));
    }

    static int ParseTemperature(ReadOnlySpan<byte> tempText)
    {
        int currentPosition = 0;
        int temp;
        int negative = 1;
        // Inspired by @yemreinci to unroll this even further
        if (tempText[currentPosition] == (byte)'-')
        {
            negative = -1;
            currentPosition++;
        }
        if (tempText[currentPosition + 1] == (byte)'.')
            temp = negative * ((tempText[currentPosition] - (byte)'0') * 10 + (tempText[currentPosition + 2] - (byte)'0'));
        else
            temp = negative * ((tempText[currentPosition] - (byte)'0') * 100 + ((tempText[currentPosition + 1] - (byte)'0') * 10 + (tempText[currentPosition + 3] - (byte)'0')));
        return temp;
    }
}
