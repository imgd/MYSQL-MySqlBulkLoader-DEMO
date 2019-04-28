using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using Microsoft.AspNetCore.Mvc;
using MySql.Data.MySqlClient;

namespace WebApplication1.Controllers
{
    [ApiController]
    [Route("[controller]/[action]")]
    public class TestController : Controller
    {
        DataContext _context;
        public TestController(DataContext context)
        {
            _context = context;
        }
        // GET: Test
        //测试10000条数据12s
        //测试100000条数据卡死
        //适用于20000条一下数据
        public JsonResult Index(int count = 100)
        {
            var Count = count;
            DateTime dt1 = DateTime.Now;
            var data = new List<peoples>();
            for (int i = 0; i < Count; i++)
            {
                _context.peoples.Add(new peoples()
                {
                    name = ("插入name" + i),
                    age = (Count + i),
                    sex = (Count + i) % 2
                });
            }
            //_context.peoples.AddRange(data);
            _context.SaveChanges();
            //_context.BulkInsert()
            DateTime dt2 = DateTime.Now;
            //data = null;
            return Json(new { Count = Count, Hs = (dt2 - dt1).TotalSeconds });
        }

        //datatable数据源
        //测试100000条数据 0.6s
        //测试1000000条数据 7s
        //适用于大批量数据
        public JsonResult Index2(int count = 100)
        {
            var Count = count;
            DateTime dt1 = DateTime.Now;
            var data = new List<peoples>();
            for (int i = 0; i < Count; i++)
            {
                data.Add(new peoples()
                {
                    name = ("插入name" + i),
                    age = (Count + i),
                    sex = (Count + i) % 2
                });
            }

            var dtable = data.ToDataTable();
            using (MySqlConnection conn =
                new MySqlConnection("server=localhost;userid=root;pwd=123456;port=3306;database=test;sslmode=none;"))
            {
                var results = conn.BulkInsert(dtable, typeof(peoples).Name.ToString());
            }


            DateTime dt2 = DateTime.Now;
            //data = null;
            return Json(new { Count = Count, Hs = (dt2 - dt1).TotalSeconds });
        }

        //IList<T>数据源
        //测试100000条数据 0.6s
        //测试1000000条数据 5.1s
        //适用于大批量数据
        public JsonResult Index3(int count = 100)
        {
            var Count = count;
            DateTime dt1 = DateTime.Now;
            var data = new List<peoples>();
            for (int i = 0; i < Count; i++)
            {
                data.Add(new peoples()
                {
                    name = ("插入name" + i),
                    age = (Count + i),
                    sex = (Count + i) % 2
                });
            }


            using (MySqlConnection conn =
                new MySqlConnection("server=localhost;userid=root;pwd=123456;port=3306;database=test;sslmode=none;"))
            {
                var results = conn.BulkInsert(data, typeof(peoples).Name.ToString());
            }

            DateTime dt2 = DateTime.Now;
            data = null;
            return Json(new { Count = Count, Hs = (dt2 - dt1).TotalSeconds });
        }

    }




    /// <summary>
    /// 注意MYSQL 开启 csv导入 必须要 设置 my.ini   secure_file_priv = 
    /// SHOW VARIABLES LIKE "secure_file_priv";
    /// http://blog.sina.com.cn/s/blog_59bba95d0102wspc.html
    /// </summary>
    public static class CSVEx
    {
        private static string RootPath = Directory.GetCurrentDirectory();

        /// <summary>
        /// 批量导入 DataTable数据源
        /// </summary>
        /// <param name="_mySqlConnection"></param>
        /// <param name="dt"></param>
        /// <returns></returns>
        public static int BulkInsert(this MySqlConnection _mySqlConnection, DataTable table, string tableName)
        {
            table.ToCsv();

            var columns = table.Columns.Cast<DataColumn>().Select(colum => colum.ColumnName).ToList();
            MySqlBulkLoader bulk = new MySqlBulkLoader(_mySqlConnection)
            {
                FieldTerminator = ",",
                FieldQuotationCharacter = '"',
                EscapeCharacter = '"',
                LineTerminator = "\r\n",
                FileName = Path.Combine(RootPath, table.TableName + ".csv"),
                NumberOfLinesToSkip = 0,
                TableName = tableName,

            };

            bulk.Columns.AddRange(columns);
            try
            {
                return bulk.Load();
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                if (File.Exists(Path.Combine(RootPath, table.TableName + ".csv")))
                {
                    File.Delete(Path.Combine(RootPath, table.TableName + ".csv"));
                }
            }

        }

        /// <summary>
        /// 批量导入 IList数据源
        /// </summary>
        /// <param name="_mySqlConnection"></param>
        /// <param name="dt"></param>
        /// <returns></returns>
        public static int BulkInsert<T>(this MySqlConnection _mySqlConnection, IList<T> data, string tableName)
        {
            string fileName = $"temp_{tableName}_{Guid.NewGuid().ToString()}.csv";
            data.ToCsv(fileName);

            var columns = typeof(T).GetProperties().Select(k => k.Name).ToList();
            MySqlBulkLoader bulk = new MySqlBulkLoader(_mySqlConnection)
            {
                FieldTerminator = ",",
                FieldQuotationCharacter = '"',
                EscapeCharacter = '"',
                LineTerminator = "\r\n",
                FileName = Path.Combine(RootPath, fileName),
                NumberOfLinesToSkip = 0,
                TableName = tableName,

            };

            bulk.Columns.AddRange(columns);

            try
            {
                return bulk.Load();
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                if (File.Exists(Path.Combine(RootPath, fileName)))
                {
                    File.Delete(Path.Combine(RootPath, fileName));
                }
            }

        }

        /// <summary>
        ///将DataTable转换为标准的CSV
        /// </summary>
        /// <param name="table">数据表</param>
        /// <returns>返回标准的CSV</returns>
        public static void ToCsv(this DataTable table)
        {
            //以半角逗号（即,）作分隔符，列为空也要表达其存在。
            //列内容如存在半角逗号（即,）则用半角引号（即""）将该字段值包含起来。
            //列内容如存在半角引号（即"）则应替换成半角双引号（""）转义，并用半角引号（即""）将该字段值包含起来。
            StringBuilder sb = new StringBuilder();
            DataColumn colum;
            foreach (DataRow row in table.Rows)
            {
                for (int i = 0; i < table.Columns.Count; i++)
                {
                    colum = table.Columns[i];
                    if (i != 0) sb.Append(",");
                    if (colum.DataType == typeof(string) && row[colum].ToString().Contains(","))
                    {
                        sb.Append("\"" + row[colum].ToString().Replace("\"", "\"\"") + "\"");
                    }
                    else sb.Append(row[colum].ToString());
                }
                sb.AppendLine();
            }

            File.WriteAllText(Path.Combine(RootPath, table.TableName + ".csv"), sb.ToString());
        }

        /// <summary>
        /// 将IList<T>转换为标准的CSV
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="data"></param>
        /// <param name="fileName"></param>
        public static void ToCsv<T>(this IList<T> data, string fileName)
        {
            //以半角逗号（即,）作分隔符，列为空也要表达其存在。
            //列内容如存在半角逗号（即,）则用半角引号（即""）将该字段值包含起来。
            //列内容如存在半角引号（即"）则应替换成半角双引号（""）转义，并用半角引号（即""）将该字段值包含起来。
            StringBuilder sb = new StringBuilder();
            var _type = typeof(T);
            var _properties = _type.GetProperties();
            foreach (T item in data)
            {
                var i = 0;
                foreach (PropertyInfo item2 in _properties)
                {
                    if (i != 0) sb.Append(",");
                    var value = _type.GetProperty(item2.Name).GetValue(item);

                    if (item2.PropertyType == typeof(string) && value.ToString().Contains(","))
                        sb.Append("\"" + value.ToString().Replace("\"", "\"\"") + "\"");
                    else
                        sb.Append(value.ToString());
                    i++;
                }

                sb.AppendLine();
            }

            File.WriteAllText(Path.Combine(RootPath, fileName), sb.ToString());

        }



    }
    public static class TableListHelper
    {

        /// <summary>  
        /// 转化一个DataTable  
        /// </summary>  
        /// <typeparam name="T"></typeparam>  
        /// <param name="list"></param>  
        /// <returns></returns>  
        public static DataTable ToDataTable<T>(this IEnumerable<T> list)
        {
            //创建属性的集合  
            List<PropertyInfo> pList = new List<PropertyInfo>();
            //获得反射的入口  
            Type type = typeof(T);
            DataTable dt = new DataTable("temp_table" + Guid.NewGuid().ToString());
            //把所有的public属性加入到集合 并添加DataTable的列  
            Array.ForEach<PropertyInfo>(type.GetProperties(), p => { pList.Add(p); dt.Columns.Add(p.Name, p.PropertyType); });
            foreach (var item in list)
            {
                //创建一个DataRow实例  
                DataRow row = dt.NewRow();
                //给row 赋值  
                pList.ForEach(p => row[p.Name] = p.GetValue(item, null));
                //加入到DataTable  
                dt.Rows.Add(row);
            }
            return dt;
        }


        /// <summary>  
        /// DataTable 转换为List 集合  
        /// </summary>  
        /// <typeparam name="TResult">类型</typeparam>  
        /// <param name="dt">DataTable</param>  
        /// <returns></returns>  
        public static List<T> ToList<T>(this DataTable dt) where T : class, new()
        {
            //创建一个属性的列表  
            List<PropertyInfo> prlist = new List<PropertyInfo>();
            //获取TResult的类型实例  反射的入口  

            Type t = typeof(T);

            //获得TResult 的所有的Public 属性 并找出TResult属性和DataTable的列名称相同的属性(PropertyInfo) 并加入到属性列表   
            Array.ForEach<PropertyInfo>(t.GetProperties(), p => { if (dt.Columns.IndexOf(p.Name) != -1) prlist.Add(p); });

            //创建返回的集合  

            List<T> oblist = new List<T>();

            foreach (DataRow row in dt.Rows)
            {
                //创建TResult的实例  
                T ob = new T();
                //找到对应的数据  并赋值  
                prlist.ForEach(p => { if (row[p.Name] != DBNull.Value) p.SetValue(ob, row[p.Name], null); });
                //放入到返回的集合中.  
                oblist.Add(ob);
            }
            return oblist;
        }
    }
}