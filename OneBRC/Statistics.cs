namespace OneBRC
{
    internal class Statistics
    {
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
