using System.Collections.Concurrent;

namespace OneBRC
{
    internal class ProcessQueues
    {
        public readonly string Path;
        public readonly int Parallelism;
        public readonly BlockingCollection<Context> ProcessingQueue;
        public readonly BlockingCollection<Context> FreeQueue;
        public readonly Context[] Contexts;
        

        public ProcessQueues(string path, int parallelism, int blockSize)
        {
            Path = path;
            Parallelism = parallelism;
            Contexts = new Context[parallelism];
            ProcessingQueue = new BlockingCollection<Context>(parallelism);
            FreeQueue = new BlockingCollection<Context>(parallelism);
            for (int i = 0; i < parallelism; i++)
            {
                var context = new Context(blockSize);
                FreeQueue.Add(context);
                Contexts[i] = context;
            }
        }
    }
}
