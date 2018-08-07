using System;
using System.IO;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using MongoDB.Driver;

namespace Import
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var importer = new PostcodeImporter.Importer();
            await importer.ImportPostcodes(new FileStream(@"c:\mongo\ukpostcodes.csv", FileMode.Open, FileAccess.Read));
            
            Console.WriteLine("Complete");
        }
    }
}