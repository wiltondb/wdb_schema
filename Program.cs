/*
 * Copyright 2024, WiltonDB Software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using CommandLine;
using Microsoft.IdentityModel.Tokens;
using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Smo;

class Program
{
  public class Options
  {
    [Option('s', "hostname", Required = true, HelpText = "Specifies the hostname of the DB to which to connect.")]
    public required string Hostname { get; set; }
    [Option('p', "port", Required = false, HelpText = "Specifies the TCP port of the DB to which to connect.")]
    public int Port { get; set; }
    [Option('n', "instance", Required = false, HelpText = "Specifies the instance name of SQL Server to which to connect.")]
    public required string Instance { get; set; }
    [Option('u', "username", Required = false, HelpText = "Specifies the login name used to connect to DB.")]
    public required string Username { get; set; }
    [Option('x', "password", Required = false, HelpText = "Specifies the password for the login ID. If this option isn't used, the password is read from WDBSCHEMAPASSWORD environment variable.")]
    public required string Password { get; set; }
    [Option('w', "windows_auth", Required = false, HelpText = "Specifies that the tool connects to DB with a trusted connection using integrated security.")]
    public bool WindowsAuth { get; set; }
    [Option('d', "database", Required = true, HelpText = "Specifies the database to connect to.")]
    public required string Database { get; set; }
    [Option('t', "table", Required = false, HelpText = "Specifies the table name to export the schema only for this table.")]
    public required string Table { get; set; }
    [Option('h', "table_schema", Required = false, HelpText = "Specifies the table schema name to export the schema only for this table.")]
    public required string TableSchema { get; set; }
    [Option('f', "functions", Required = false, HelpText = "Specifies that the functions and stored procedures must be included into the exported schema.")]
    public bool Functions { get; set; }
  }

  static void Main(string[] args)
  {
    Parser.Default.ParseArguments<Options>(args)
      .WithParsed(TryRunScripter)
      .WithNotParsed(ExitOnError);
  }

  static void ExitOnError(IEnumerable<Error> enumerable)
  {
    Environment.Exit(1);
  }

  static void TryRunScripter(Options options)
  {
    try
    {
      RunScripter(options);
      Environment.Exit(0);
    }
    catch (Exception e)
    {
      Console.Error.WriteLine("ERROR: " + e.Message);
      Environment.Exit(1);
    }
  }

  static void RunScripter(Options opts)
  {
    var conn = OpenConn(opts);
    var server = new Server(conn);
    var db = server.Databases[opts.Database];
    var objects = new List<SqlSmoObject>();

    if (opts.Table.IsNullOrEmpty())
    {
      foreach (Table t in db.Tables) objects.Add(t);
      foreach (View v in db.Views) objects.Add(v);
      if (opts.Functions)
      {
        foreach (StoredProcedure p in db.StoredProcedures) objects.Add(p);
        foreach (UserDefinedFunction f in db.UserDefinedFunctions) objects.Add(f);
      }
    }
    else
    {
      foreach (Table t in db.Tables)
      {
        if (t.Name == opts.Table)
        {
          if (!opts.TableSchema.IsNullOrEmpty() && opts.TableSchema != t.Schema)
          {
            continue;
          }
          objects.Add(t);
          break;
        }
      }
    }

    ExportObjects(server, objects);
  }

  static ServerConnection OpenConn(Options opts)
  {
    var sname = !opts.Instance.IsNullOrEmpty() ?
        opts.Hostname + "\\" + opts.Instance :
        opts.Hostname + "," + opts.Port;
    if (opts.WindowsAuth)
    {
      return new ServerConnection(sname);
    }
    else
    {
      var envPwd = Environment.GetEnvironmentVariable("WDBSCHEMAPASSWORD");
      envPwd = !envPwd.IsNullOrEmpty() ? envPwd : "";
      var pwd = !opts.Password.IsNullOrEmpty() ? opts.Password : envPwd;
      return new ServerConnection(sname, opts.Username, pwd);
    }
  }

  static void ExportObjects(Server server, List<SqlSmoObject> objects)
  {
    if (objects.Count == 0)
    {
      throw new Exception("No DB objects are selected for export.");
    }

    var scripter = new Scripter(server);
    scripter.Options.AllowSystemObjects = false;
    scripter.Options.DriAll = true;
    scripter.Options.ExtendedProperties = true;
    scripter.Options.IncludeDatabaseContext = false;
    scripter.Options.Indexes = true;
    scripter.Options.ScriptSchema = true;
    scripter.Options.ScriptData = false;
    scripter.Options.NoCollation = true;
    scripter.Options.Triggers = true;

    var exclusions = new List<string> {
      "SET ANSI_NULLS ON",
      "SET QUOTED_IDENTIFIER ON"
    };

    var lst = scripter.Script(objects.ToArray());

    foreach (var line in lst)
    {
      if (null == line || exclusions.Contains(line))
      {
        continue;
      }
      Console.WriteLine(line);
      Console.WriteLine("GO");
      Console.WriteLine("");
    }
  }

}