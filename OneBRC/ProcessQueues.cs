using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OneBRC
{
    internal class ProcessQueues
    {
        public readonly int parallelism;
        public readonly BlockingCollection<Context> processingQueue;
        public readonly BlockingCollection<Context> freeQueue;
        public readonly Context[] Contexts;
        

        public ProcessQueues(int parallelism)
        {
            this.parallelism = parallelism;
            Contexts = new Context[parallelism];
            processingQueue = new BlockingCollection<Context>(parallelism);
            freeQueue = new BlockingCollection<Context>(parallelism);
            for (int i = 0; i < parallelism; i++)
            {
                var context = new Context(new Dictionary<int, string>(512), new List<string>(512));
                freeQueue.Add(context);
                Contexts[i] = context;
            }
        }
    }
}
