using Ionic.Zip;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using System.Xml.Serialization;

namespace DB
{
    [Serializable]
    public class Celula
    {
        public void Set(string valor)
        {
            this.Valor = valor;
        }
        public void Set(double valor)
        {
            this.Valor = valor.ToString().Replace(",",".");
        }
        public void Set(Valor valor)
        {
            this.Valor = valor.valor;
        }
        public void Set(int valor)
        {
            this.Valor = valor.ToString().Replace(",", ".");
        }
        public void Set(bool valor)
        {
            this.Valor = valor.ToString().Replace(",", ".");
        }
        public void Set(DateTime valor)
        {
            this.Valor = valor.ToShortDateString();
        }
        public Valor Get()
        {
            return new Valor(Valor);
        }
        public override string ToString()
        {
            return "[" + Coluna + "]=" + Valor;
        }
        public string Tabela { get; set; } = "";
        public string Coluna { get; set; } = "";
        public string Valor { get; set; } = "";
        public byte[] arquivo()
        {
            if(File.Exists(Valor) && isArquivo)
            {
                int FileSize;

                FileStream fs;

                fs = new FileStream(this.Valor, FileMode.Open, FileAccess.Read);
                FileSize = Convert.ToInt32(fs.Length);

                var T = new byte[FileSize];
                fs.Read(T, 0, FileSize);
                fs.Close();
                return T;
            }
            return null;
        
        }
        public bool isArquivo { get; set; } = false;

        public Celula()
        {

        }
        public Celula(string Valor)
        {
            this.Valor = Valor;
            this.Coluna = Coluna;
        }
        public Celula(string Coluna, string Valor)
        {
            this.Valor = Valor;
            this.Coluna = Coluna;
        }

        public Celula(string Coluna, double Valor)
        {
            this.Valor = Valor.ToString().Replace(",",".");
            this.Coluna = Coluna;
        }
        public Celula(string Coluna, int Valor)
        {
            this.Valor = Valor.ToString();
            this.Coluna = Coluna;
        }
        public Celula(string Coluna, bool Valor)
        {
            this.Valor = Valor.ToString();
            this.Coluna = Coluna;
        }
        public Celula(string Coluna, object Valor)
        {
            this.Valor = Valor.ToString();
            this.Coluna = Coluna;
        }

        public Celula(string Coluna, DateTime Valor)
        {
            this.Valor = Valor.ToShortDateString();
            this.Coluna = Coluna;
        }
    }

    [Serializable]
    public class Linha
    {
       public List<string> Valores()
        {
            return this.Celulas.Select(x => x.Valor).ToList();
        }
        public static List<List<Linha>> QuebraLista(List<Linha> Lista, int Tamanho = 1000)
        {
            var list = new List<List<Linha>>();

            for (int i = 0; i < Lista.Count; i += Tamanho)
            {
                list.Add(Lista.GetRange(i, Math.Min(Tamanho, Lista.Count - i)));
            }

            return list;
        }
        public override string ToString()
        {
            return (Tabela!=""?"Tabela: " + Tabela:"") + " Células: " + this.Celulas.Count;
        }
        public string Tabela { get; set; } = "";
        public void Add(string coluna, string valor)
        {
            var t = this.Celulas.Find(x => x.Coluna.ToUpper() == coluna.ToUpper());
            if(t!=null)
            {
                t.Set(valor);
                return;
            }
            this.Celulas.Add(new Celula(coluna, valor));
        }
        public void Add(string coluna, double valor)
        {
            var t = this.Celulas.Find(x => x.Coluna.ToUpper() == coluna.ToUpper());
            if (t != null)
            {
                t.Set(valor);
                return;
            }
            this.Celulas.Add(new Celula(coluna, valor));
        }
        public void Add(string coluna, int valor)
        {
            var t = this.Celulas.Find(x => x.Coluna.ToUpper() == coluna.ToUpper());
            if (t != null)
            {
                t.Set(valor);
                return;
            }
            this.Celulas.Add(new Celula(coluna, valor));
        }
        public void Add(string coluna, bool valor)
        {
            var t = this.Celulas.Find(x => x.Coluna.ToUpper() == coluna.ToUpper());
            if (t != null)
            {
                t.Set(valor);
                return;
            }
            this.Celulas.Add(new Celula(coluna, valor));
        }
        public void Add(string coluna, DateTime valor)
        {
            var t = this.Celulas.Find(x => x.Coluna.ToUpper() == coluna.ToUpper());
            if (t != null)
            {
                t.Set(valor);
                return;
            }
            this.Celulas.Add(new Celula(coluna, valor));
        }
        public Valor Get(string Coluna)
        {
            try
            {
                var s = Celulas.Find(x => x.Coluna.ToUpper() == Coluna.ToUpper());
                if (s != null)
                {
                    return new Valor(s.Valor);
                   
                }
                else {
                    return new Valor("");
                }

            }
            catch (Exception)
            {
                return new Valor("");
            }
            
        }

        public void Set(string Coluna,string valor)
        {

            var Retorno = Celulas.Find(x => x.Coluna.ToUpper() == Coluna.ToUpper());
            if (Retorno != null)
            {
                Retorno.Set(valor);
            }
        }
        public void Set(string Coluna, double valor)
        {

            var Retorno = Celulas.Find(x => x.Coluna.ToUpper() == Coluna.ToUpper());
            if (Retorno != null)
            {
                Retorno.Set(valor.ToString().Replace(",","."));
            }
        }
        public void Set(string Coluna, bool valor)
        {

            var Retorno = Celulas.Find(x => x.Coluna.ToUpper() == Coluna.ToUpper());
            if (Retorno != null)
            {
                Retorno.Set(valor.ToString());
            }
        }
        public void Set(string Coluna, int valor)
        {

            var Retorno = Celulas.Find(x => x.Coluna.ToUpper() == Coluna.ToUpper());
            if (Retorno != null)
            {
                Retorno.Set(valor.ToString());
            }
        }

        public List<Celula> GetValores(string Coluna, string Valor, bool exato = true)
        {
            List<Celula> Retorno = Celulas.FindAll(x => x.Coluna.ToUpper() == Coluna.ToUpper());
            try
            {
                if (exato)
                {
                    Retorno = Retorno.FindAll(x => x.Valor.ToUpper().Contains(Valor.ToUpper()));
                }
                else
                {
                    Retorno = Retorno.FindAll(x => x.Valor.ToUpper().Contains(Valor.ToUpper()));
                   
                }

            }
            catch (Exception)
            {
         
            }
            return Retorno;
        }
        public string GetValor(string Coluna, bool exato = true)
        {
            Celula Retorno = null;
            try
            {
                if(exato)
                {
                    Retorno = Celulas.Find(x => x.Coluna.ToUpper() == Coluna.ToUpper());
                    if (Retorno == null)
                    {
                        return "";
                    }
                    else
                    {
                        return Retorno.Valor;
                    }
                }
                else
                {
                    Retorno = Celulas.Find(x => x.Coluna.ToUpper().Contains(Coluna.ToUpper()));
                    if (Retorno == null)
                    {
                        return "";
                    }
                    else
                    {
                        return Retorno.Valor;
                    }
                }
               
            }
            catch (Exception)
            {
                return "";
            }

        }
        [XmlIgnore]
        public List<string> Header
        {
            get
            {
                return Celulas.Select(x => x.Coluna).ToList();
            }
        }

        public List<Celula> Celulas { get; set; } = new List<Celula>();
        
        public Linha(string Tabela)
        {
            this.Tabela = Tabela;
        }   
        public Linha(string Tabela, List<string> Colunas, MySqlDataReader sqlDataReader)
        {
            this.Tabela = Tabela;

            foreach (string Coluna in Colunas)
            {
                Celula n = new Celula(Coluna, sqlDataReader[Coluna].ToString());
                this.Celulas.Add(n);
            }
        }
        public Linha()
        {

        }
        public Linha(List<Celula> celulas)
        {
            this.Celulas = celulas;
        }
    }
    
    public class Tabela
    {
        public override string ToString()
        {
            return "[" + Nome + "]" + "/L:" + Linhas.Count();
        }
        public string Nome { get; set; } = "";

        public Tabela Carregar(string Arquivo)
        {

            if (File.Exists(Arquivo))
            {
                try
                {
                    using (ZipFile zip = ZipFile.Read(Arquivo))
                    {
                        ZipEntry e = zip["tabela.dbdlm"];
                        if (zip.Entries.Count > 0)
                        {

                            XmlSerializer x = new XmlSerializer(typeof(Tabela));
                            Tabela ts = (Tabela)x.Deserialize(zip.Entries.ToArray()[0].OpenReader());
                            this.Linhas = ts.Linhas;
                            this.Nome = ts.Nome;
                            return ts;

                        }

                    }
                }
                catch (Exception ex)
                {
                    MessageBox.Show("Erro descompactando arquivo .dbdlm\n" + ex.Message + "\n" + ex.StackTrace);
                }
            }
            return new Tabela();
        }

        public bool Gravar(string Arquivo)
        {
            try
            {

                string arquivo_tmp = Application.StartupPath + @"\tabela.dbdlm";

                if (File.Exists(arquivo_tmp))
                {
                    File.Delete(arquivo_tmp);
                }

                XmlSerializer x = new XmlSerializer(typeof(Tabela));
                TextWriter writer = new StreamWriter(arquivo_tmp);
                x.Serialize(writer, this);
                writer.Close();

                if (File.Exists(Arquivo))
                {
                    File.Delete(Arquivo);
                }
                using (ZipFile zip = new ZipFile())
                {

                    zip.AddFile(arquivo_tmp, "");

                    zip.Save(Arquivo);
                }
                File.Delete(arquivo_tmp);
                return true;
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Erro Tentando criar o arquivo {Arquivo}\n" + ex.Message + "\n" + ex.StackTrace);
                return false;
            }

        }


        public List<string> GetColunas()
        {
            return Linhas.SelectMany(x => x.Header).Distinct().ToList();
        }

        public List<Linha> Linhas { get; set; } = new List<Linha>();

        public List<Linha> Filtrar  (string Chave, string Valor, bool exato =false)
        {
            List<Linha> Retorno = new List<Linha>();
            if(exato)
            {
                return Linhas.FindAll(x => x.Celulas.FindAll(y => y.Coluna == Chave && y.Valor == Valor).Count>0);
            }
            else
            {
                return Linhas.FindAll(x => x.Celulas.FindAll(y => y.Coluna.ToLower().Replace(" ","") == Chave.ToLower().Replace(" ", "") && y.Valor.ToLower().Replace(" ", "").Contains(Valor.ToLower().Replace(" ", ""))).Count > 0);
            }          
        }

        public Tabela(List<Linha> Linhas, string Nome)
        {
            this.Nome = Nome;
            this.Linhas = Linhas;
        }

        public Tabela(string Tabela)
        {
            this.Nome = Tabela;
        }

        public Tabela Filtro(string Chave,string Valor,bool Exato)
        {
            return new Tabela(Filtrar(Chave, Valor, Exato),Nome);
        }

        public Tabela()
        {

        }
    }

    public class Valor
    {
        public override string ToString()
        {
            return valor;
        }
        public DateTime Data
        {
            get
            {
                return DBUtilz.Data(valor);
            }
        }
        public bool Boolean
        {
            get
            {
                return DBUtilz.Boolean(valor);
            }
        }
        public double Double(int Decimais = 4)
        {
            return DBUtilz.Double(valor, Decimais);
        }
        public int Int
        {
            get
            {
                string comps = valor;
                if (comps == "") { comps = "0"; }
                try
                {
                    return Convert.ToInt32(Math.Ceiling(Double()));
                }
                catch (Exception)
                {

                    return 0;
                }
            }
        }
        public string valor { get; set; } = "";
        public Valor(string valor)
        {
            this.valor = valor;
        }
        public Valor()
        {

        }
    }

}
