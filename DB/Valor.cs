using System;

namespace DB
{
    public class Valor
    {
        public bool existe { get; set; } = false;
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
        public long Long
        {
            get
            {
                string comps = valor;
                if (comps == "") { comps = "0"; }
                try
                {
                    return Convert.ToInt64(Math.Ceiling(Double()));
                }
                catch (Exception)
                {

                    return 0;
                }
            }
        }
        public string valor { get; set; } = "";
        public Valor(string valor, bool existe)
        {
            this.valor = valor;
            this.existe = existe;
        }
        public Valor()
        {

        }
    }

}
