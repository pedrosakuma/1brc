using System.Numerics;

namespace OneBRC
{
    internal class Statistics
    {
        public int Count;
        public int ItemIndex;
        public readonly float[] Items = new float[Vector<float>.Count];
        public Vector<float> Sum;
        public Vector<float> Min = new Vector<float>(float.MaxValue);
        public Vector<float> Max = new Vector<float>(float.MinValue);

        internal void Add(float value)
        {
            Count++;
            int currentIndex = ItemIndex++;
            Items[currentIndex] = value;
            if (currentIndex == Items.Length - 1)
            {
                var currentVector = new Vector<float>(Items);
                Sum = Vector.Add(Sum, currentVector);
                Min = Vector.Min(Min, currentVector);
                Max = Vector.Max(Max, currentVector);
                ItemIndex = 0;
            }
        }
    }
}
