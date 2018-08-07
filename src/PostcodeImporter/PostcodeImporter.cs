using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using CsvHelper.TypeConversion;
using MongoDB;
using MongoDB.Driver;

namespace PostcodeImporter
{
    
    internal class PostcodeData
    {
        
        public long Id { get; set; }
        public string Postcode { get; set; }
        public double Latitude { get; set; }
        public double Longitude { get; set; }
    }
    
    public class Importer
    {
        public async Task ImportPostcodes(Stream csvStream)
        {
            using (var streamReader = new StreamReader(csvStream))
            {
                using (var csvReader = new CsvHelper.CsvReader(streamReader))
                {
                   
                    var client = new MongoClient("mongodb://localhost:32773");
                    var database = client.GetDatabase("HousePrice");
                    database.DropCollection("Postcodes");
                    var collection = database.GetCollection<PostcodeData>("Postcodes");
			
                    var batch = new List<PostcodeData>();
                    long lastId = 0;
                    
                    while( csvReader.Read() )
                    {
                        try
                        {
                            var record = csvReader.GetRecord<PostcodeData>();
                            record.Postcode = record.Postcode.Replace(" ", String.Empty);
                            lastId = record.Id;
                            batch.Add(record);

                        }

                        catch (TypeConverterException)
                        {
                            // invalid lat/long so ignore
                        }
                        
                        if (batch.Count == 1000)
                        {
                            await collection.InsertManyAsync(batch);
                            batch.Clear();
                        }
                            
                    }

                    if (batch.Any())
                    {
                        await collection.InsertManyAsync(batch);
                    }

                    Console.WriteLine("Generating index...");
                    var notificationLogBuilder = Builders<PostcodeData>.IndexKeys;
                    var indexModel = new CreateIndexModel<PostcodeData>(notificationLogBuilder.Ascending(x => x.Postcode));
                    await collection.Indexes.CreateOneAsync(indexModel).ConfigureAwait(false);
                }
            }
        }
    }
}