namespace OneBRC
{
    internal class Statistics
    {
        public readonly Utf8StringUnsafe Key;
        public Statistics(Utf8StringUnsafe key)
        {
            Key = key;
        }
        public int Count;
        public long Sum;
        public int Min;
        public int Max;

        internal void Add(int value)
        {
            Count++;
            Sum += value;
            Min = int.Min(Min, value);
            Max = int.Max(Max, value);
        }
    }
}
