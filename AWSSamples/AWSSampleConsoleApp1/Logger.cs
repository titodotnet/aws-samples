using System.Diagnostics;

namespace AWSSampleConsoleApp1
{
    public class Logger : ILogger
    {
        public void WriteLine(string message)
        {
            Debug.WriteLine(message);
        }
    }

    public interface ILogger
    {
        void WriteLine(string message);
    }
}
