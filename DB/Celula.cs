using System;
using System.Globalization;
using System.IO;
using System.Text;

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
            return new Valor(Valor,true);
        }
        public override string ToString()
        {
            return "[" + Coluna + "]=" + Valor;
        }
        public string Tabela { get; set; } = "";
        public string Coluna { get; set; } = "";
        public string Valor { get; set; } = "";


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
            this.Valor = Valor.ToString().Replace(",",".");
            this.Coluna = Coluna;
        }

        public Celula(string Coluna, DateTime Valor)
        {
            this.Valor = Valor.ToShortDateString();
            this.Coluna = Coluna;
        }
    }

}
