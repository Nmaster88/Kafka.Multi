using ConsumerApp.Models;
using Microsoft.EntityFrameworkCore;
namespace ConsumerApp.DataAccess.TestDb
{
    public class TestDbContext : DbContext
    {
        public DbSet<TopicMessages> Messages { get; set; }

        public DbSet<TopicMessagesJson> messagesJson { get; set; }
        protected override void OnModelCreating(ModelBuilder builder)
        {
            builder.Entity<TopicMessagesJson>().HasKey(m => m.id);
            base.OnModelCreating(builder);
        }
        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            string constr = Utility.GetConnectionString("ConnectionStrings:DefaultConnectionEF");
            optionsBuilder.UseNpgsql(constr);
        }
    }
}