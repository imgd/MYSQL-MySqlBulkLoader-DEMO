using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace WebApplication1
{
    public class DataContext : DbContext
    {
        public DbSet<peoples> peoples { get; set; }
        public DataContext(DbContextOptions<DataContext> options) : base(options)
        {

        }
    }

    public class peoples
    {
        public int id { get; set; }
        public string name { get; set; }
        public int age { get; set; }
        public int sex { get; set; }
    }
}
