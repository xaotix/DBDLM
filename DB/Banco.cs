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


        public void Log(string Mensagem)
        {
            Log(Mensagem, ArquivoLog);
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
                Log("Erro tentando conectar:" + this.ToString(), ex);
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
        public DataTable Retorno { get; set; }
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
                Log("Erro"+ "\nComando:" + Comando + "\n\n",ex);
            }
            return MySQLComando;

        }

        public List<string> GetDbases()
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
        public List<string> GetTabelas(string Database)
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

        internal static List<Colunas> colunas { get; set; } = new List<Colunas>();
        public List<string> GetColunas(string Database, string Tabela)
        {
            BancoDeDados = Database;
            TabelaAtual = Tabela;
            var colunas = new List<string>();
            try
            {
                var ret = DB.Banco.colunas.Find(x => x.servidor.ToUpper() == this.Servidor.ToUpper() && x.database.ToUpper() == Database.ToUpper() && x.tabela.ToUpper() == Tabela.ToUpper() && x.colunas.Count > 0);

                if (ret != null)
                {
                    colunas.AddRange(ret.colunas);
                    return colunas;
                }
                else
                {
                    var instrucaoSQL = "select column_name from information_schema.columns where table_name='" + Tabela + "' and table_schema = '" + BancoDeDados + "'";
                    MySqlConnection con = null;

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

                        DB.Banco.colunas.Add(new Colunas(this.Servidor, Database, Tabela, colunas));
                    }

                    Desconectar(con);
                }

            }
            catch (Exception ex)
            {
                Log("Erro", ex);
            }
            return colunas;
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
  


        public Tabela Consulta(Celula Criterio, bool Exato, string Database, string Tabela)
        {
            List<Linha> Retorno = new List<Linha>();
           var  Colunas = GetColunas(Database, Tabela);
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
        public Tabela Consulta(List<Celula> Criterios, bool Exato, string Database, string Tabela, string condicional = "And")
        {
            List<Linha> Retorno = new List<Linha>();
            var Colunas = GetColunas(Database, Tabela);
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

        public bool Apagar(List<Celula> Filtro, string Database, string Tabela, string condicional = "And")
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


            List<string> Colunas = GetColunas(Database, Tabela);
            Filtro = Filtro.FindAll(x => Colunas.Find(y => y.ToUpper() == x.Coluna.ToUpper()) != null);
            if(Filtro.Count>0)
            {
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
               var cc = ExecutarComando(ComandoFIM);
                return cc != null;
            }

            return false;
          
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="Valores">Lista de celula com os valores a ser Cadastrados</param>
        /// <param name="Database">Banco de Dados onde os dados serão Armazenados</param>
        /// <param name="Tabela">Tabela onde os dados serão Armazenados</param>
        /// <returns></returns>
        public long Cadastro(List<Celula> Valores, string Database, string Tabela)
        {
            
            TabelaAtual = Valores[0].Tabela;

            


            try
            {
                List<string> Colunas = GetColunas(Database, Tabela);

                if(Colunas.Count==0)
                {
                    return -1;
                }
                string Comando = Comando = $"INSERT INTO {Database}.{Tabela} (";
                Valores = Valores.FindAll(x => Colunas.Find(y => y.ToUpper() == x.Coluna.ToUpper()) != null);
                string Columns = "";
                string Vals = "";
                if(Valores.Count==0)
                {
                    return -1;
                }
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


        public bool Cadastro(List<Linha> linhas, string Database, string Tabela)
        {
            linhas = linhas.FindAll(x => x != null);
            bool retorno = true;

            if(linhas.Count==0)
            {
                return true;
            }
            if(Database=="")
            {
                return false;
            }
            if(Tabela=="")
            {
                return false;
            }

            if (Database == null)
            {
                Database = this.BancoDeDados;
            }
            if (Tabela == null)
            {
                Tabela = this.TabelaAtual;
            }


            //Verifica se há algum endereço de arquivo para cadastrar
            try
            {
                List<string> Colunas =  GetColunas(Database, Tabela);
                if(Colunas.Count==0)
                {
                    Log($"Comando 'Cadastro' com linhas: ao procurar colunas {Servidor} = {Database}.{Tabela} retornou zero.");
                    return false;
                }

                //lista todas as colunas das linhas
                var cols_linhas = linhas.SelectMany(x => x.Celulas).ToList().FindAll(x=>x!=null).Select(x => x.Coluna.ToUpper()).Distinct().ToList().OrderBy(x => x).ToList();


                var cols_existem = cols_linhas.FindAll(x => Colunas.Find(y => y.ToUpper() == x.ToUpper()) != null);

                var pacotes = DBUtilz.quebrar_lista(linhas, Max_Registos_Simultaneos);


                if(cols_existem.Count>0)
                {
                    string Comando_Colunas = Comando_Colunas = $"INSERT INTO {Database}.{Tabela} (";
                    for (int i = 0; i < cols_existem.Count; i++)
                    {
                        Comando_Colunas = Comando_Colunas + $"`{cols_existem[i]}`" ;
                        if(i<cols_existem.Count-1)
                        {
                            Comando_Colunas = Comando_Colunas + ", ";
                        }
                    }

                    Comando_Colunas = Comando_Colunas + ") values ";


                    foreach(var pacote in pacotes)
                    {
                        string comando_valores = "";

                        foreach (var l in pacote)
                        {

                            string comando_linha = "";
                            for (int i = 0; i < cols_existem.Count; i++)
                            {
                                var igual = l.Get(cols_existem[i]);
                                if(igual.existe)
                                {
                                    comando_linha = comando_linha + $"'{MySql.Data.MySqlClient.MySqlHelper.EscapeString(igual.valor.Replace(",", "."))}'";
                                }
                                else
                                {
                                    comando_linha = comando_linha + "NULL";
                                }
                                if (i < cols_existem.Count - 1)
                                {
                                    comando_linha = comando_linha + ", ";
                                }
                            }
                            if(comando_linha!="")
                            {
                                comando_valores = comando_valores + (comando_valores!=""?", ":"") + $"({comando_linha})";
                            }
                        }

                       
                        if(comando_valores!="")
                        {
                            string comando_final = Comando_Colunas + comando_valores;
                            var Tab = new DataTable();
                            MySqlConnection con;
                            MySqlCommand cc = ExecutarComando(comando_final,ref Tab,out con);
                            Desconectar(con);
                            if (cc.LastInsertedId>0)
                            {
                                foreach(var l in pacote)
                                {
                                    l.Cadastrou = true;
                                }
                            }
                            else
                            {
                                Log($"Comando 'Cadastro' com linhas:  {Servidor} = {Database}.{Tabela} não fez registros.\nComando:\n\n{comando_final}\n\n");
                                retorno = false;
                            }
                        }
                        else
                        {
                            Log($"Comando 'Cadastro' com linhas:  {Servidor} = {Database}.{Tabela} não conseguiu montar a lista para registros.");
                            retorno = false;
                        }
                    }
                }
                else
                {
                    Log($"Comando 'Cadastro' com linhas:  {Servidor} = {Database}.{Tabela} Nenhuma das colunas setadas existe na tabela: " +
                        $"\n[Colunas setadas:]\n" +
                        $"{string.Join(", ",cols_linhas)}" +
                        $"\n\n[Colunas na tabela:]\n" +
                        $"{string.Join(", ",Colunas)}");
                }

                
               

              

            }
            catch (Exception ex)
            {
                Log("Erro", ex);
                retorno = false;
            }

            if(!retorno)
            {
              if(File.Exists(ArquivoLog))
                {
                    try
                    {
                       
                        Process.Start(ArquivoLog);
                    }
                    catch (Exception)
                    {

                    }
                }
            }
            return retorno;

        }

        public int Max_Registos_Simultaneos { get; set; } = 30;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ChavesFiltro">Parâmetros para a Pesquisa (Where)</param>
        /// <param name="ColunasAEditar">Colunas que devem ser Alteradas</param>
        /// <param name="Database">Banco de Dados onde os dados estão Armazenados</param>
        /// <param name="Tabela">Tabela onde os dados estão Armazenados</param>
        public void Update(List<Celula> ChavesFiltro, List<Celula> ColunasAEditar, string Database , string Tabela)
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

            List<string> Colunas  = GetColunas(Database, Tabela);
            for (int i = 0; i < ColunasAEditar.Count; i++)
            {
                Comando = Comando + "`" + ColunasAEditar[i].Coluna + "` = '" + MySql.Data.MySqlClient.MySqlHelper.EscapeString(ColunasAEditar[i].Valor) + "'";

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
            ComandoFIM = "UPDATE " + Database + "." + Tabela + " SET " + Comando + " Where " + chaveComando;
            MySqlConnection con;
            ExecutarComando(ComandoFIM, ref TableBuffer, out con);
            Desconectar(con);


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
        public void Update(string ChaveFiltro, string ValorFiltro, List<Celula> ColunasAEditar, string Database, string Tabela, bool Exato = true)
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

            List<string> Colunas = GetColunas(Database, Tabela);
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
    }
}
//



