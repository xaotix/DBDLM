using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Xml.Serialization;

namespace DB
{
    [Serializable]
    public class Linha
    {
        public override string ToString()
        {
            return (Descricao!=""? Descricao : (Tabela!=""?"Tabela: " + Tabela:"")) + " Células: " + this.Celulas.Count;
        }
        public Linha Clonar()
        {
            DB.Linha retorno = new Linha();
            retorno.Cadastrou = this.Cadastrou;
            retorno.Descricao = this.Descricao;
            retorno.Tabela = this.Tabela;
            foreach(var c in this.Celulas)
            {
                retorno.Celulas.Add(new Celula(c.Coluna, c.Valor));
            }
            return retorno;
        }
        public string Descricao { get; set; } = "";
        public string Tabela { get; set; } = "";
        public bool Cadastrou { get; set; } = false;
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
                var s = Celulas.FindAll(x=>x!=null).Find(x => x.Coluna.ToUpper() == Coluna.ToUpper());
                if (s != null)
                {
                    return new Valor(s.Valor, true);

                }
                else
                {
                    return new Valor("", false);
                }

            }
            catch (Exception)
            {
                return new Valor("",false);
            }
            
        }

        public void Set(string Coluna,string valor)
        {
            Add(Coluna, valor);
        }
        public void Set(string Coluna, double valor)
        {
            Add(Coluna, valor);
        }
        public void Set(string Coluna, bool valor)
        {
            Add(Coluna, valor);
        }
        public void Set(string Coluna, int valor)
        {
            Add(Coluna, valor);
        }
        public void Set(string Coluna, DateTime valor)
        {
            Add(Coluna, valor);
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

        public List<string> GetColunas()
        {
            return Celulas.Select(x => x.Coluna).ToList();
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

}
