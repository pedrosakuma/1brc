using System.Buffers;
using System.Diagnostics;
using System.Numerics;

namespace OneBRC;

class Program
{
    static void Main(string[] args)
    {
        var sw = Stopwatch.StartNew();
        ProcessQueues processQueues = new ProcessQueues(18);

        var producer = new Thread(Produce);
        producer.Start(processQueues);
        
        var consumers = Enumerable.Range(0, processQueues.parallelism)
            .Select(_ => new Thread(Consume)).ToArray();

        foreach (var consumer in consumers)
            consumer.Start(processQueues);

        producer.Join();
        foreach (var consumer in consumers)
            consumer.Join();

        WriteOrderedStatistics(processQueues.Contexts.First().ordered, GroupAndAggregateStatistics(processQueues));
        Console.WriteLine(sw.Elapsed);
    }

    private static void WriteOrderedStatistics(List<string> ordered, Dictionary<string, FlattenedStatistics> final)
    {
        bool first = true;
        Console.Write("{");
        foreach (var item in ordered)
        {
            FlattenedStatistics statistics = final[item];
            if (first)
                first = false;
            else
                Console.Write(", ");

            Console.Write($"{item}={statistics.Min.ToString("0.0")}/{((float)statistics.Sum / statistics.Count).ToString("0.0")}/{statistics.Max.ToString("0.0")}");
        }
        Console.WriteLine("}");
    }

    private static Dictionary<string, FlattenedStatistics> GroupAndAggregateStatistics(ProcessQueues processQueues)
    {
        Dictionary<string, FlattenedStatistics> final = new Dictionary<string, FlattenedStatistics>();
        foreach (var context in processQueues.Contexts)
        {
            foreach (var data in context.data)
            {
                if (!final.TryGetValue(data.Key, out var stats))
                {
                    stats = new FlattenedStatistics();
                    final.Add(data.Key, stats);
                }
                stats.Count += data.Value.Count;
                for (int i = 0; i < Vector<float>.Count; i++)
                {
                    stats.Sum += data.Value.Sum[i];
                    stats.Min = MathF.Min(stats.Min, data.Value.Min[i]);
                    stats.Max = MathF.Max(stats.Max, data.Value.Max[i]);
                }
                for (int i = 0; i < data.Value.ItemIndex; i++)
                {
                    stats.Sum += data.Value.Items[i];
                    stats.Min = MathF.Min(stats.Min, data.Value.Items[i]);
                    stats.Max = MathF.Max(stats.Max, data.Value.Items[i]);
                }
            }
        }

        return final;
    }

    private static void Consume(object? data)
    {
        ArgumentNullException.ThrowIfNull(data);
        var p = (ProcessQueues)data;
        Context? context;
        while (p.processingQueue.TryTake(out context, -1))
        {
            var span = context.BlockBuffer.AsSpan(0, context.BlockBufferSize);
            context.LinesCount = GetLines(context.Indexes, context.Lengths, span);

            for (int i = 0; i < context.LinesCount; i++)
            {
                ReadOnlySpan<byte> line = span
                    .Slice(context.Indexes[i], context.Lengths[i]);
                ProcessMessage(context, line);
            }
            if (context != null && !p.freeQueue.IsAddingCompleted)
                p.freeQueue.Add(context);
        }
    }

    private static void Produce(object? data)
    {
        ArgumentNullException.ThrowIfNull(data);
        var p = (ProcessQueues)data;
        using (var measurements = File.OpenRead("C:\\Users\\pedrotravi\\source\\repos\\1brc\\measurements.txt"))
        {
            int readOffset = 0;
            byte[] remainder = new byte[256];
            while (p.freeQueue.TryTake(out var context, -1))
            {
                var blockBuffer = context.BlockBuffer;

                remainder.AsSpan(0, readOffset).CopyTo(blockBuffer);
                int size = measurements.Read(blockBuffer, readOffset, blockBuffer.Length - readOffset);
                if (size == 0)
                {
                    p.processingQueue.CompleteAdding();
                    p.freeQueue.CompleteAdding();
                    break;
                }
                context.BlockBufferSize = size + readOffset;

                var span = blockBuffer.AsSpan(0, context.BlockBufferSize);

                int lastIndexOfLineBreak = span.LastIndexOf((byte)'\n');

                p.processingQueue.Add(context);

                readOffset = context.BlockBufferSize - (lastIndexOfLineBreak + 1);
                span.Slice(lastIndexOfLineBreak + 1).CopyTo(remainder);
            }
        }
    }

    static int GetLines(int[] indexes, int[] lengths, Span<byte> span)
    {
        int lineCount = 0;
        int offset = 0;
        while (true)
        {
            var lineBreakIndex = span.IndexOf((byte)'\n');
            if (lineBreakIndex == -1)
                break;

            var currentCount = lineCount++;
            indexes[currentCount] = offset;
            lengths[currentCount] = lineBreakIndex;

            span = span.Slice(lineBreakIndex + 1);
            offset += lineBreakIndex + 1;
        }
        return lineCount;
    }

   
    static void ProcessMessage(Context context, ReadOnlySpan<byte> span)
    {
        var separator = span.IndexOf((byte)';');
        context.GetOrAdd(span.Slice(0, separator)).Add(
            ParseTemperature(
                span.Slice(separator + 1, span.Length - (separator + 1) - 1)));
    }

    static float ParseTemperature(ReadOnlySpan<byte> tempText)
    {
        int currentPosition = 0;
        int temp;
        int negative = 1;
        // Inspired by @yemreinci to unroll this even further
        if (tempText[currentPosition] == '-')
        {
            negative = -1;
            currentPosition++;
        }
        if (tempText[currentPosition + 1] == '.')
            temp = negative * ((tempText[currentPosition] - '0') * 10 + (tempText[currentPosition + 2] - '0'));
        else
            temp = negative * ((tempText[currentPosition] - '0') * 100 + ((tempText[currentPosition + 1] - '0') * 10 + (tempText[currentPosition + 3] - '0')));
        return temp / 10.0f;
    }
}
