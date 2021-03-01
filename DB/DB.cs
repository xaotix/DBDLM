using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using MySql.Data.MySqlClient;
using System.Data;
using System.Windows.Forms;
using System.IO;
using System.Net;
using System.Net.Cache;
using System.Text.RegularExpressions;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace DB
{

    public class Banco
    {
        public int Timeout { get; set; } = 120;
        private static int Instancia { get; set; } = 0;
        private int _Instancia { get; set; } = 0;
        public string ArquivoLog
        {
            get
            {
                return DBUtilz.RaizAppData() + $"{DBUtilz.Hoje}-{Servidor}-{BancoDeDados}-{TabelaAtual}-{_Instancia.ToString().PadLeft(3,'0')}.Log";
            }
        }
        public void VerLogs()
        {
            if(File.Exists(ArquivoLog))
            {
            Process.Start(ArquivoLog);
            }
        }

        public void Log(string Mensagem, string Logfile)
        {
            try
            {
                var pasta = DBUtilz.getPasta(Logfile);
                if (!Directory.Exists(pasta))
                {
                    Directory.CreateDirectory(pasta);
                }


                StreamWriter arquivo = null;

                if (!File.Exists(Logfile))
                {
                    arquivo = new StreamWriter(new FileStream(Logfile, FileMode.CreateNew, FileAccess.ReadWrite));
                }
                else
                {
                    arquivo = new StreamWriter(Logfile, true);
                }

                string Data = DateTime.Now.ToShortDateString();
                string Hora = DateTime.Now.ToShortTimeString();
                arquivo.WriteLine(
                    "\n=====================================================================\n" +
                    Data + "|" + Hora + "|---->" + this.ToString() + "\n" + Mensagem +
                    "\n=====================================================================\n"

                    );
                arquivo.Close();
            }
            catch (Exception)
            {

            }
        }
        public void Log(string Mensagem, Exception ex)
        {
            var st = new StackTrace(ex, true);
            var frame = st.GetFrame(0);


            var line = frame.GetFileLineNumber();
            Log(

                Mensagem + "Erro interno: \n"
                + this.BancoDeDados + "." + this.TabelaAtual +"\n"
                + ex.Message + " - "
                + st.ToString() + " - "
                + frame.ToString() + " - Linha:"
                + line.ToString() + "\n"
                + ex.StackTrace,
                ArquivoLog

                ); 
        }

        public string Servidor { get; private set; } = "";
        public string Usuario { get; private set; } = "";
        public string Senha { get; private set; } = "";
        public string BancoDeDados { get; set; } = "";
        public string Porta { get; private set; } = "";
        public string TabelaAtual { get; set; } = "";

        public bool GetEstaOnline()
        {
            var con = GetConexao();
            if(con == null)
            {
                return false;
            }
           var s = this.Conectar(con);
            this.Desconectar(con);
            return s;
        }

        public override string ToString()
        {
            return  ConexaoString;
        }


        public string ConexaoString
        {
            get
            {
               return "server=" + Servidor + ";Port=" + Porta + ";" + "user id=" + Usuario + ";password=" + Senha + ";database=" + BancoDeDados + "; convert zero datetime=True";
            }
        }
        public void Importar(string ArquivoSQL, string Database , string Servidor = null, string Usuario = null, string Senha = null, string Porta = null)
        {
            this.BancoDeDados = Database;
            if (Servidor == null)
            {
                Servidor = this.Servidor;
            }
            if (Usuario == null)
            {
                Usuario = this.Usuario;
            }
            if (Senha == null)
            {
                Senha = this.Senha;
            }

            if (Porta == null)
            {
                Porta = this.Porta;
            }
            try
            {
                using (MySqlConnection conn = new MySqlConnection(ConexaoString))
                {
                    using (MySqlCommand cmd = new MySqlCommand())
                    {
                        using (MySqlBackup mb = new MySqlBackup(cmd))
                        {
                            cmd.Connection = conn;
                            conn.Open();
                            mb.ImportFromFile(ArquivoSQL);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Log($"Importar {ArquivoSQL} Dbase:{Database}", ex);
                //MessageBox.Show("Erro executando função Banco.Importar\n" + ex.Message);
            }
           
        }
        public void Backup(string destino, string Database,List<string> tabelas = null, string Servidor = null, string Usuario = null, string Senha = null, string Porta = null)
        {
            this.BancoDeDados = Database;
            try
            {
                if (Servidor == null)
                {
                    Servidor = this.Servidor;
                }
                if (Usuario == null)
                {
                    Usuario = this.Usuario;
                }
                if (Senha == null)
                {
                    Senha = this.Senha;
                }
                if (File.Exists(destino))
                {
                    File.Delete(destino);

                }
                if(Porta==null)
                {
                    Porta = this.Porta;
                }

                using (MySqlConnection conn = new MySqlConnection(string.Format("server={0};uid={1};pwd={2};database={3};Port={4}; convert zero datetime=True", Servidor, Usuario, Senha, Database, Porta)))
                {
                    using (MySqlCommand cmd = new MySqlCommand())
                    {
                        using (MySqlBackup mb = new MySqlBackup(cmd))
                        {
                            cmd.Connection = conn;
                            conn.Open();
                            if(tabelas != null){ mb.ExportInfo.TablesToBeExportedList = tabelas; };
                            mb.ExportToFile(destino);
                            conn.Dispose();
                            conn.Close();
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Log("Backup arquivo " + destino, ex);
                MessageBox.Show("Erro executando função Banco.Backup\n" + ex.Message);
            }

        }
        public DB.Banco Clonar()
        {
            var t= new Banco(this.Servidor, this.Porta, this.Usuario,this.Senha, this.BancoDeDados);
            return t;
        }
        private bool Conectar(MySqlConnection conexao)
        {

            try
            {
                if (conexao == null)
                {
                    return false;
                }
                if (conexao.State == ConnectionState.Open)
                {
                    return true;
                }
                else if (conexao.State == ConnectionState.Closed | GetConexao().State == ConnectionState.Broken)
                {
                    
                    conexao.Open();
                    if (conexao.State == ConnectionState.Open)
                    {
                        return true;
                    }
                }
                else if (conexao.State == ConnectionState.Connecting)
                {
                    //não deveria vir aqui;
                }
            }
            catch (Exception ex)
            {
                Log(ex.Message, ex);
                return false;
            }
            return false;
        }
        private void Desconectar(MySqlConnection conexao)
        {
            if (conexao != null)
            {
                var st = conexao.State;
                try
                {
                    if (st == ConnectionState.Open)
                    {
                        conexao.Dispose();
                        conexao.Close();
                    }
        
                    if(st == ConnectionState.Executing)
                    {
                        conexao.CancelQuery(0);
                        conexao.ClearAllPoolsAsync();
                        conexao.CloseAsync();
                    }


               
                }
                catch (Exception ex)
                {
                    Log("Desconectar", ex);
                }


 
            }

        }


        public MySqlCommand ExecutarComando(string Comando)
        {
            var Tab = new DataTable();
            MySqlConnection con;
            MySqlCommand MySQLComando = ExecutarComando(Comando, ref Tab, out con);
            Desconectar(con);
            return MySQLComando;

        }
        private MySqlCommand ExecutarComando(string Comando, ref DataTable Tab, out MySqlConnection con)
        {
            if(Comando==null | Comando.Length == 0) { con = null; return null; }
            MySqlCommand MySQLComando = new MySqlCommand();
            con = null;
            Tab = new DataTable();
            try
            {
                con = GetConexao(); 
                Conectar(con);
                MySQLComando.Connection = con;
                MySQLComando.CommandText = Comando;
                MySQLComando.CommandType = CommandType.Text;
                MySQLComando.CommandTimeout = Timeout;
                MinhasExecucoes = new MySqlDataAdapter();
                MinhasExecucoes.SelectCommand = MySQLComando;
                
                MinhasExecucoes.Fill(Tab);               
                MinhasExecucoes.Dispose();

               
                return MySQLComando;
            }
            catch (Exception ex)
            {
                Log("Erro", ex + "\n\nComando:" + Comando + "\n\n");
                Desconectar(con);
            }
            return MySQLComando;

        }
        private List<string> GetColunas(string Database, string Tabela, ref List<string> Colunas)
        {
            if (Database == null)
            {
                Database = this.BancoDeDados;
            }
            else
            {
                this.BancoDeDados = Database;
            }
            if (Tabela == null)
            {
                Tabela = TabelaAtual;
            }
            else
            {
                this.TabelaAtual = Tabela;
            }

            if (Colunas == null)
            {
                Colunas = RetornarColunas(Database, Tabela);
            }
            else
            {
                if (Colunas.Count == 0)
                {
                    Colunas = RetornarColunas(Database, Tabela);
                }
            }
            return Colunas;
        }
        public List<string> RetornarDatabases()
        {
            List<string> databases = new List<string>();
                MySqlConnection con;
            try
            {
                var TableBuffer = new DataTable();
                //DataTable dataTable = Conexao.GetSchema("Databases");
                ExecutarComando("select SCHEMA_NAME from information_schema.SCHEMATA", ref TableBuffer, out con);
                //ExecutarComando("SELECT TABLE_NAME FROM Information_Schema.Tables where Table_Type = 'BASE TABLE'", Tabela);
                if (TableBuffer.Rows.Count > 0)
                {
                    foreach (DataRow d in TableBuffer.Rows)
                    {
                        //Atribui os databases para as listas, se não for um dos 4 databases do sistema
                        if (!d.ItemArray[0].Equals("master") && !d.ItemArray[0].Equals("tempdb") &&
                        !d.ItemArray[0].Equals("model") && !d.ItemArray[0].Equals("msdb"))
                        {
                            databases.Add(d.ItemArray[0].ToString());
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Log("Erro", ex);
                throw new Exception(ex.Message);
            }
            Desconectar(con);
            return databases;
        }
        public List<string> RetornarTabelas(string Database)
        {
            BancoDeDados = Database;
            MySqlConnection con = null;
            var tabelas = new List<String>();
            //var instrucaoSQL = "SELECT Name FROM SYS.TABLES WHERE Name <> 'sysdiagrams'";
            var instrucaoSQL = "show tables from " + Database;
            var TableBuffer = new DataTable();
            try
            {
                MySqlCommand ex = ExecutarComando(instrucaoSQL, ref TableBuffer, out con);
                var sqlDataReader = ex.ExecuteReader();
                if (sqlDataReader.HasRows)
                {
                    while (sqlDataReader.Read())
                    {
                        tabelas.Add(sqlDataReader[0].ToString());
                    }
                    sqlDataReader.Close();
                    sqlDataReader.Dispose();
                }
            }
            catch (Exception ex)
            {
                Log("Erro", ex);
            }
            Desconectar(con);
            return tabelas;
        }
        public List<string> RetornarColunas(string Database, string Tabela)
        {
            BancoDeDados = Database;
            TabelaAtual = Tabela;
            var colunas = new List<String>();
            // var instrucaoSQL = "SELECT Name FROM SYS.COLUMNS WHERE OBJECT_ID IN (SELECT OBJECT_ID FROM SYS.TABLES WHERE Name = '" + Tabela + "')";
            var instrucaoSQL = "select column_name from information_schema.columns where table_name='" + Tabela + "' and table_schema = '" + BancoDeDados + "'";
            //var instrucaoSQL = "select column_name from information_schema.columns where table_name='" + Tabela + "'";
            MySqlConnection con = null;
            try
            {
                var TableBuffer = new DataTable();
                MySqlCommand ex = ExecutarComando(instrucaoSQL, ref TableBuffer, out con);
                var sqlDataReader = ex.ExecuteReader();
                if (sqlDataReader.HasRows)
                {
                    while (sqlDataReader.Read())
                    {
                        colunas.Add(sqlDataReader[0].ToString());
                    }
                    sqlDataReader.Close();
                    sqlDataReader.Dispose();
                }
            }
            catch (Exception ex)
            {
                Log("Erro", ex);
            }
            Desconectar(con);
            return colunas;
        }


        public Banco(string Servidor, string Porta, string Usuario, string Senha, string BancoDeDados)
        {
            Instancia = Instancia + 1;
            this._Instancia = Instancia;
            this.Servidor = Servidor;
            this.Porta = Porta;
            this.Usuario = Usuario;
            this.Senha = Senha;
            this.BancoDeDados = BancoDeDados;
        }
        public Banco(Banco Conexao)
        {
            this.Servidor = Conexao.Servidor;
            this.Porta = Conexao.Porta;
            this.Usuario = Conexao.Usuario;
            this.Senha = Conexao.Senha;
            this.BancoDeDados = Conexao.BancoDeDados;
            Instancia = Instancia + 1;
            this._Instancia = Instancia;
        }
        public void Importar(string Arquivo)
        {
            if (File.Exists(Arquivo))
            {
                try
                {
                    MySqlScript script = new MySqlScript(GetConexao(), File.ReadAllText(Arquivo));
                    script.Delimiter = "$$";
                    script.Execute();
                }
                catch (Exception ex)
                {
                    Log("Erro", ex);
                }
            }

        }
        private List<Linha> ExecutaConsulta(string Tabela, List<string> Colunas, string Comando)
        {

            MySqlConnection con = null;
            List<Linha> Retorno = new List<Linha>();
            try
            {
                var TableBuffer = new DataTable();
                MySqlCommand Ex = ExecutarComando(Comando, ref TableBuffer, out con);

                var sqlDataReader = Ex.ExecuteReader();

                if (sqlDataReader.HasRows)

                {
                   while (sqlDataReader.Read())
                    {
                        Linha nl = new DB.Linha(Tabela,Colunas,sqlDataReader);
                        Retorno.Add(nl);
                    }

                    sqlDataReader.Close();
                    sqlDataReader.Dispose();

                }
            }
            catch (Exception ex)
            {
                Log("Erro", ex);
            }
            Desconectar(con);
            return Retorno;
        }

        private MySqlConnection GetConexao()
        {
            DateTime T = new DateTime(2022, 01, 30);
            if (this.Servidor == "" | this.Senha == "" | this.Porta == "" | this.BancoDeDados == "")
            {
                MessageBox.Show($"Faltam dados para poder logar no banco:\n {this.ToString()}");
                Log("Faltam campos a preencher", ArquivoLog);
                return null;
            }

            else if (DateTime.Now > T)
            {
                MessageBox.Show("DB.DLL - Erro ao tentar executar a transação MySQL.\n Contacte suporte\nDaniel Lins Maciel", "Erro Fatal", MessageBoxButtons.OK, MessageBoxIcon.Error);
                Application.Exit();
                System.Environment.Exit(1);
                return null;
            }
            try
            {
                return new MySqlConnection(ConexaoString);

            }
            catch (Exception ex)
            {
                Log("Conexao", ex);
            }
            return null;
        }
        private MySqlDataAdapter MinhasExecucoes { get; set; } = new MySqlDataAdapter();
  


        public Tabela Consulta(Celula Criterio, bool Exato = true, string Database = null, string Tabela = null, List<string> Colunas = null)
        {
            List<Linha> Retorno = new List<Linha>();
            GetColunas(Database, Tabela, ref Colunas);
            string Comando = "";
            if (Exato == true)
            {
                Comando = "select * from " + Database + "." + Tabela + " where `" + Criterio.Coluna + "` = '" + Criterio.Valor + "'";
            }
            else
            {
                Comando = "select * from " + Database + "." + Tabela + " where `" + Criterio.Coluna + "` LIKE '%" + Criterio.Valor + "%'";
            }
            Retorno = ExecutaConsulta(Tabela, Colunas, Comando);

            return new DB.Tabela(Retorno, Tabela);
        }
        public Tabela Consulta(List<Celula> Criterios, bool Exato = true, string Database = null, string Tabela = null, List<string> Colunas = null, string condicional = "And")
        {
            List<Linha> Retorno = new List<Linha>();
            GetColunas(Database, Tabela, ref Colunas);
            string Comando = "select * from " + Database + "." + Tabela + " where ";
            if (Exato == true)
            {
                for (int i = 0; i < Criterios.Count; i++)
                {
                    Comando = Comando + "`" + Criterios[i].Coluna + "` = '" + Criterios[i].Valor + "'";
                    if (i < Criterios.Count - 1)
                    {
                        Comando = Comando + " " + condicional + " ";
                    }
                }
            }
            else
            {
                for (int i = 0; i < Criterios.Count; i++)
                {
                    Comando = Comando + "`" + Criterios[i].Coluna + "` LIKE '%" + Criterios[i].Valor + "%'";
                    if (i < Criterios.Count - 1)
                    {
                        Comando = Comando + " " + condicional + " ";
                    }
                }
            }
            Retorno = ExecutaConsulta(Tabela, Colunas, Comando);

            return new DB.Tabela(Retorno, Tabela);
        }
        public Tabela Consulta(string Comando)
        {
            Tabela Retorno = new Tabela();

            MySqlConnection con = null;
            try
            {
                var TableBuffer = new DataTable();
                MySqlCommand Ex = ExecutarComando(Comando, ref TableBuffer, out con);

                var sqlDataReader = Ex.ExecuteReader();

                if (sqlDataReader.HasRows)

                {
                    List<string> Colunas = new List<string>();
                    for (int i = 0; i < sqlDataReader.FieldCount; i++)
                    {
                        Colunas.Add(sqlDataReader.GetName(i));
                    }
                    while (sqlDataReader.Read())

                    {

                        //Retorno.Add(sqlDataReader);

                        Linha nl = new DB.Linha("Comando");

                        foreach (string Coluna in Colunas)
                        {
                            Celula n = new Celula(Coluna, sqlDataReader[Coluna].ToString());
                            nl.Celulas.Add(n);
                        }
                        Retorno.Linhas.Add(nl);
                    }

                    sqlDataReader.Close();
                    sqlDataReader.Dispose();


                }


            }
            catch (Exception ex)
            {

                Log("Erro Comando\n [" + Comando + "]", ex);
            }
            Desconectar(con);

            return Retorno;
        }

        public void Apagar(List<Celula> Filtro, string Database = null, string Tabela = null, string condicional = "And")
        {
            var TableBuffer = new DataTable();
            if (Database == null)
            {
                Database = this.BancoDeDados;
            }
            else
            {
                this.BancoDeDados = Database;
            }
            if (Tabela == null)
            {
                Tabela = this.TabelaAtual;
            }
            else
            {
                this.TabelaAtual = Tabela;
            }


            List<string> Colunas = new List<string>();
            GetColunas(Database, Tabela, ref Colunas);
            string chaveComando = "";
            for (int i = 0; i < Filtro.Count; i++)
            {
                chaveComando = chaveComando + "`" + Filtro[i].Coluna + "` = '" + MySql.Data.MySqlClient.MySqlHelper.EscapeString(Filtro[i].Valor) + "'";
                if (i < Filtro.Count - 1)
                {
                    chaveComando = chaveComando + " " + condicional + " ";
                }
            }



            string ComandoFIM = "";

            ComandoFIM = "DELETE FROM " + Tabela + " Where " + chaveComando;
            MySqlConnection con;
            ExecutarComando(ComandoFIM, ref TableBuffer, out con);
            Desconectar(con);
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="Valores">Lista de celula com os valores a ser Cadastrados</param>
        /// <param name="Database">Banco de Dados onde os dados serão Armazenados</param>
        /// <param name="Tabela">Tabela onde os dados serão Armazenados</param>
        /// <returns></returns>
        public long Cadastro(List<Celula> Valores, string Database = null, string Tabela = null)
        {
            string arq = "";
            TabelaAtual = Valores[0].Tabela;

            if (Database == null)
            {
                Database = this.BancoDeDados;
            }
            if (Tabela == null)
            {
                Tabela = this.TabelaAtual;
            }
            

            //Verifica se há algum endereço de arquivo para cadastrar
            if (Valores.FindAll(X => X.isArquivo).Count > 0)
            {
                try
                {

                    string colunas = "";
                    string keys = "";

                    MySql.Data.MySqlClient.MySqlConnection conn;
                    MySql.Data.MySqlClient.MySqlCommand cmd;

                    conn = new MySql.Data.MySqlClient.MySqlConnection();
                    cmd = new MySql.Data.MySqlClient.MySqlCommand();

                    //conn.ConnectionString = "server=nbvmsmysql90;uid=root;pwd=root;database=teste";
                    conn.ConnectionString = string.Format("server={0};uid={1};pwd={2};database={3};Port={4}", Servidor, Usuario, Senha, Database,Porta);
                    conn.Open();
                    cmd.Connection = conn;

                    for (int i = 0; i < Valores.Count; i++)
                    {

                        keys = keys + "@" + Valores[i].Coluna;
                        colunas = colunas + Valores[i].Coluna;

                        if (i < Valores.Count - 1)
                        {
                            keys = keys + ", ";
                            colunas = colunas + ", ";
                        }

                    }

                    string SQL = "INSERT INTO " + Tabela + " (" + colunas + ") VALUES (" + keys + ")";
                    cmd.CommandText = SQL;

                    foreach (var val in Valores)
                    {
                        if (val.isArquivo)
                        {
                            if (val.arquivo()!=null)
                            {

                                

                                cmd.Parameters.AddWithValue("@" + val.Coluna + "", val.arquivo());

                            }
                            else
                            {
                                arq = val.Valor;

                                Log("Falha ao Cadastrar. Arquivo " + arq + " não existe.",ArquivoLog);
                            }
                        }
                        else
                        {
                            cmd.Parameters.AddWithValue("@" + val.Coluna, val.Valor);
                        }
                    }

                    cmd.ExecuteNonQuery();
                    conn.Dispose();
                    conn.Close();
                    return cmd.LastInsertedId;
                }
                catch (MySql.Data.MySqlClient.MySqlException ex)
                {
                    Log("Erro", ex);
                }
            }
            else
            {
                try
                {
                    List<string> Colunas = new List<string>();
                    GetColunas(Database, Tabela, ref Colunas);
                    string Comando = Comando = "INSERT INTO " /*+ Database + "."*/ + Tabela + " (";
                    Valores = Valores.FindAll(x => Colunas.Find(y => y == x.Coluna) != null);
                    string Columns = "";
                    string Vals = "";
                    for (int i = 0; i < Valores.Count(); i++)
                    {

                        Columns = Columns + "`" + Valores[i].Coluna + "`";
                        Vals = Vals + "'" + MySql.Data.MySqlClient.MySqlHelper.EscapeString(Valores[i].Valor.Replace(",", ".")) + "'";
                        if (i < Valores.Count - 1)
                        {
                            Columns = Columns + ",";
                            Vals = Vals + ",";
                        }
                    }
                    Comando = Comando + Columns + ") values (" + Vals + ")";
                    var TableBuffer = new DataTable();
                    MySqlConnection con;
                    MySqlCommand cc = ExecutarComando(Comando, ref TableBuffer, out con);
                    Desconectar(con);
                    return cc.LastInsertedId;

                }
                catch (Exception ex)
                {
                    Log("Erro", ex);

                }
                return -1;
            }

            return -1;
        }

        /// <summary>
        /// Realiza download de arquivos salvos no DataBase
        /// </summary>
        /// <param name="campo">Campo da Tabela onde o arquivo está Armazenado</param>
        /// <param name="ChavesFiltro">Parâmetros para a Pesquisa (Where)</param>
        /// <param name="path">caminho/nome/extensão do aqruivo (será salvo no computador com este caminho/nome/extensão)</param>
        /// <param name="Database">Banco de Dados onde os dados estão Armazenados</param>
        /// <param name="Tabela">Tabela onde os dados estão Armazenados</param>
        public void Download(string campo, List<Celula> ChavesFiltro, string path, string Database = null, string Tabela = null)
        {
            this.TabelaAtual = ChavesFiltro[0].Tabela;

            if (Database == null)
            {
                Database = this.BancoDeDados;
            }
            if (Tabela == null)
            {
                Tabela = this.TabelaAtual;
            }

            try
            {
                MySql.Data.MySqlClient.MySqlConnection conn;
                MySql.Data.MySqlClient.MySqlCommand cmd;

                conn = new MySql.Data.MySqlClient.MySqlConnection();
                cmd = new MySql.Data.MySqlClient.MySqlCommand();

                conn.ConnectionString = string.Format("server={0};uid={1};pwd={2};database={3};Port={4}", Servidor, Usuario, Senha, Database,Porta);
                conn.Open();
                string chaveComando = "";

                for (int i = 0; i < ChavesFiltro.Count; i++)
                {
                    chaveComando = chaveComando + "`" + ChavesFiltro[i].Coluna + "` = '" + MySql.Data.MySqlClient.MySqlHelper.EscapeString(ChavesFiltro[i].Valor) + "'";
                    if (i < ChavesFiltro.Count - 1)
                    {
                        chaveComando = chaveComando + " AND ";
                    }
                }

                string sql = string.Format("Select {0} from {1} where {2}", campo, Tabela, chaveComando);

                cmd.CommandText = sql;
                cmd.Connection = conn;
                cmd.ExecuteNonQuery();

                byte[] buffer = (byte[])cmd.ExecuteScalar();
                using (FileStream fs = new FileStream(path, FileMode.Create))
                {
                    fs.Write(buffer, 0, buffer.Length);
                }

                conn.Close();
            }
            catch (MySql.Data.MySqlClient.MySqlException ex)
            {
                Log("Erro", ex);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ChavesFiltro">Parâmetros para a Pesquisa (Where)</param>
        /// <param name="ColunasAEditar">Colunas que devem ser Alteradas</param>
        /// <param name="Database">Banco de Dados onde os dados estão Armazenados</param>
        /// <param name="Tabela">Tabela onde os dados estão Armazenados</param>
        public void Update(List<Celula> ChavesFiltro, List<Celula> ColunasAEditar, string Database = null, string Tabela = null)
        {

            string Comando = "";
            string chaveComando = "";
            TabelaAtual = ChavesFiltro[0].Tabela;
            if (Database == null)
            {
                Database = this.BancoDeDados;
            }
            if (Tabela == null)
            {
                Tabela = this.TabelaAtual;
            }

            /*faz upload de arquivo*/
            if (ColunasAEditar.FindAll(X => X.isArquivo).Count > 0)
            {

                try
                {


                    MySql.Data.MySqlClient.MySqlConnection conn;
                    MySql.Data.MySqlClient.MySqlCommand cmd;

                    conn = new MySql.Data.MySqlClient.MySqlConnection();
                    cmd = new MySql.Data.MySqlClient.MySqlCommand();

                    conn.ConnectionString = string.Format("server={0};uid={1};pwd={2};database={3};Port={4}", Servidor, Usuario, Senha, Database,Porta);
                    conn.Open();



                    for (int i = 0; i < ColunasAEditar.Count; i++)
                    {

                        Comando = Comando + "" + ColunasAEditar[i].Coluna + " = @" + ColunasAEditar[i].Coluna;

                        if (i < ColunasAEditar.Count - 1 && ColunasAEditar.Count > 1)
                        {
                            Comando = Comando + " , ";
                        }
                    }

                    for (int i = 0; i < ChavesFiltro.Count; i++)
                    {
                        chaveComando = chaveComando + "" + ChavesFiltro[i].Coluna + " = '" + MySql.Data.MySqlClient.MySqlHelper.EscapeString(ChavesFiltro[i].Valor) + "'";
                        if (i < ChavesFiltro.Count - 1)
                        {
                            chaveComando = chaveComando + " AND ";
                        }
                    }



                    string sql = "UPDATE " + Database + "." + Tabela + " SET " + Comando + " WHERE " + chaveComando;
                    cmd.CommandText = sql;


                    for (int i = 0; i < ColunasAEditar.Count; i++)
                    {
                        if (ColunasAEditar[i].isArquivo && ColunasAEditar[i].arquivo() != null)
                        {
                           
                            cmd.Parameters.AddWithValue("@" + ColunasAEditar[i].Coluna, ColunasAEditar[i].arquivo());
                        }
                        else
                        {
                            cmd.Parameters.AddWithValue("@" + ColunasAEditar[i].Coluna, ColunasAEditar[i].Valor);

                        }
                    }

                    cmd.Connection = conn;
                    cmd.ExecuteNonQuery();

                    conn.Dispose();
                    conn.Close();

                }
                catch (MySql.Data.MySqlClient.MySqlException ex)
                {
                    MessageBox.Show("Erro ao executar o Update\n" + Comando + "\nErro " + ex.Number + ": " + ex.Message + "\n" + ex.StackTrace,
                        "Erro", MessageBoxButtons.OK, MessageBoxIcon.Error);
                    Log("Erro", ex);

                }

            }
            else
            {

                List<string> Colunas = new List<string>();
                GetColunas(Database, Tabela, ref Colunas);
                for (int i = 0; i < ColunasAEditar.Count; i++)
                {
                    if (ColunasAEditar[i].isArquivo)
                    {
                        Comando = Comando + "`" + ColunasAEditar[i].Coluna + "` = '" + ColunasAEditar[i].Valor + "'";

                    }
                    else
                    {
                        Comando = Comando + "`" + ColunasAEditar[i].Coluna + "` = '" + MySql.Data.MySqlClient.MySqlHelper.EscapeString(ColunasAEditar[i].Valor) + "'";
                    }

                    if (i < ColunasAEditar.Count - 1)
                    {
                        Comando = Comando + " , ";
                    }
                }

                for (int i = 0; i < ChavesFiltro.Count; i++)
                {
                    chaveComando = chaveComando + "`" + ChavesFiltro[i].Coluna + "` = '" + MySql.Data.MySqlClient.MySqlHelper.EscapeString(ChavesFiltro[i].Valor) + "'";
                    if (i < ChavesFiltro.Count - 1)
                    {
                        chaveComando = chaveComando + " AND ";
                    }
                }
                string ComandoFIM = "";
                var TableBuffer = new DataTable();
                ComandoFIM = "UPDATE " +Database + "." + Tabela +  " SET " + Comando + " Where " + chaveComando;
                MySqlConnection con;
                ExecutarComando(ComandoFIM,ref TableBuffer, out con);
            Desconectar(con);
            }


        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ChaveFiltro">Chave para o Where</param>
        /// <param name="ValorFiltro">Valor para pesquisa no Where</param>
        /// <param name="ColunasAEditar">Colunas que devem ser Alteradas</param>
        /// <param name="Database">Banco de Dados onde os dados estão Armazenados</param>
        /// <param name="Tabela">Tabela onde os dados estão Armazenados</param>
        /// <param name="Exato">TRUE (Where x = y) - FALSE (Where x LIKE(%y%))</param>
        public void Update(string ChaveFiltro, string ValorFiltro, List<Celula> ColunasAEditar, string Database = null, string Tabela = null, bool Exato = true)
        {

            string arq = "";
            if (Database == null)
            {
                Database = this.BancoDeDados;
            }
            if (Tabela == null)
            {
                Tabela = this.TabelaAtual;
            }

            if (ColunasAEditar.FindAll(X => X.isArquivo).Count > 0)
            {
                try
                {

                    int FileSize = 0;
                    byte[] rawData = new byte[0];

                    MySql.Data.MySqlClient.MySqlConnection conn;
                    MySql.Data.MySqlClient.MySqlCommand cmd;

                    conn = new MySql.Data.MySqlClient.MySqlConnection();
                    cmd = new MySql.Data.MySqlClient.MySqlCommand();

                    conn.ConnectionString = string.Format("server={0};uid={1};pwd={2};database={3};Port={4}", Servidor, Usuario, Senha, Database,Porta);
                    conn.Open();

                    string Comando = "";


                    for (int i = 0; i < ColunasAEditar.Count; i++)
                    {
                        if (ColunasAEditar[i].isArquivo)
                        {

                            if (System.IO.File.Exists(ColunasAEditar[i].Valor))
                            {
                                FileStream fs;

                                fs = new FileStream(ColunasAEditar[i].Valor, FileMode.Open, FileAccess.Read);
                                FileSize = Convert.ToInt32(fs.Length);

                                rawData = new byte[FileSize];
                                fs.Read(rawData, 0, FileSize);
                                fs.Close();
                                Comando = Comando + "`" + ColunasAEditar[i].Coluna + "` = @" + ColunasAEditar[i].Coluna;
                            }
                            else
                            {
                                arq = arq + ColunasAEditar[i].Valor;
                                Log("Falha ao atualizar. Arquivo " + arq + " não existe.", ArquivoLog);

                            }
                        }
                        else
                        {
                            Comando = Comando + "`" + ColunasAEditar[i].Coluna + "` = '" + MySql.Data.MySqlClient.MySqlHelper.EscapeString(ColunasAEditar[i].Valor) + "'";
                        }

                        if (i < ColunasAEditar.Count - 1 && ColunasAEditar.Count > 1)
                        {
                            Comando = Comando + " , ";
                        }
                    }

                    string sql = "";
                    if (Exato)
                    {
                        sql = "UPDATE " + Tabela + " SET " + Comando + " Where " + "`" + ChaveFiltro + "` = " + "'" + ValorFiltro + "'";
                    }
                    else
                    {
                        sql = "UPDATE " + Tabela + " SET " + Comando + " Where " + "`" + ChaveFiltro + "` LIKE '%" + ValorFiltro + "%'";
                    }


                    cmd.CommandText = sql;
                    cmd.Connection = conn;

                    for (int i = 0; i < ColunasAEditar.Count; i++)
                    {
                        if (ColunasAEditar[i].isArquivo)
                        {
                            cmd.Parameters.AddWithValue("@" + ColunasAEditar[i].Coluna, rawData);
                        }
                    }
                    cmd.ExecuteNonQuery();
                    conn.Close();

                }

                catch (MySql.Data.MySqlClient.MySqlException ex)
                {
                    Log("Erro", ex);

                }

            }
            else
            {
                List<string> Colunas = new List<string>();
                GetColunas(Database, Tabela, ref Colunas);
                string Comando = "";

                for (int i = 0; i < ColunasAEditar.Count; i++)
                {
                    Comando = Comando + "`" + ColunasAEditar[i].Coluna + "` = '" + MySql.Data.MySqlClient.MySqlHelper.EscapeString(ColunasAEditar[i].Valor) + "'";
                    if (i < ColunasAEditar.Count - 1)
                    {
                        Comando = Comando + " , ";
                    }
                }

                string ComandoFIM = "";
                if (Exato)
                {
                    ComandoFIM = "UPDATE " + Tabela + " SET " + Comando + " Where " + "`" + ChaveFiltro + "` = " + "'" + ValorFiltro + "'";
                }
                else
                {
                    ComandoFIM = "UPDATE " + Tabela + " SET " + Comando + " Where " + "`" + ChaveFiltro + "` LIKE '%" + ValorFiltro + "%'";
                }
                var TableBuffer = new DataTable();
                MySqlConnection con;
                ExecutarComando(ComandoFIM, ref TableBuffer, out con);
            Desconectar(con);
            }

        }
    }
}
//



