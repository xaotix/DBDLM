using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;

namespace DB
{
    internal static class DBUtilz
    {
        public static List<List<T>> quebrar_lista<T>(this List<T> locations, int maximo = 30)
        {
            var list = new List<List<T>>();

            for (int i = 0; i < locations.Count; i += maximo)
            {
                list.Add(locations.GetRange(i, Math.Min(maximo, locations.Count - i)));
            }

            return list;
        }
        public static string RaizAppData()
        {
            return CriarPasta(Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData) + @"\", "DB" + System.Windows.Forms.Application.ProductVersion);
        }
        public static string getPasta(string Arquivo)
        {
            try
            {
                return System.IO.Path.GetDirectoryName(Arquivo);

            }
            catch (Exception)
            {


            }
            return "";
        }
        public static string CriarPasta(string Raiz, string Pasta)
        {
            if (!Raiz.EndsWith(@"\")) { Raiz = Raiz + @"\"; }
            string PastaFim = (Raiz + @"\" + Pasta).Replace(@"\\", @"\");
            if (PastaFim.StartsWith(@"\")) { PastaFim = @"\" + PastaFim; }
            if (Directory.Exists(PastaFim) == false)
            {
                try
                {
                    Directory.CreateDirectory(PastaFim);
                    return PastaFim + @"\";
                }
                catch (Exception)
                {
                    return "";
                }

            }
            return PastaFim + @"\";
        }
        public static string Hoje
        {
            get
            {
                return DateTime.Now.ToShortDateString().Replace(@"/", "-").Replace(@"\", "-");
            }
        }


        public static DateTime Data(string Data)
        {
            try
            {
                return Convert.ToDateTime(Data);
            }
            catch (Exception)
            {


            }
            return new DateTime(1, 1, 1);
        }
        private static System.Globalization.CultureInfo US = new System.Globalization.CultureInfo("en-US");
        private static System.Globalization.CultureInfo BR = new System.Globalization.CultureInfo("pt-BR");

        public static double Double(object comp, int Decimais = 4)
        {

            System.Globalization.CultureInfo US = new System.Globalization.CultureInfo("en-US");
            System.Globalization.CultureInfo BR = new System.Globalization.CultureInfo("pt-BR");

            var cc = comp.ToString().ToUpper();


            cc = RemoveCaracteres(cc);

            double val;
            if (cc.Contains(","))
            {
                if (double.TryParse(cc, System.Globalization.NumberStyles.Float | NumberStyles.AllowExponent | NumberStyles.AllowDecimalPoint, BR, out val))
                {

                    return val;
                }
            }

            if (double.TryParse(cc, System.Globalization.NumberStyles.Float | NumberStyles.AllowExponent | NumberStyles.AllowDecimalPoint, US, out val))
            {

                return val;
            }

            else return 0;



        }
        private static string RemoveCaracteres(string cc)
        {
            var ss2 = cc.Replace(" ", "");
            cc = cc
                                    .Replace("(", "")
                                    .Replace(")", "")
                                    .Replace("_", "")
                                    .Replace("|", "")
                                    .Replace(@"\", "")
                                    .Replace("/", "")
                                    //.Replace("+", "")
                                    .Replace("-", "")
                                    .Replace("/", "")
                                    .Replace("{", "")
                                    .Replace("}", "")
                                    .Replace("[", "")
                                    .Replace("]", "")
                                    .Replace("&", "")
                                    .Replace("*", "")
                                    .Replace("%", "")
                                    .Replace("$", "")
                                    .Replace("#", "")
                                    .Replace("!", "")
                                    .Replace("@", "")
                                    .Replace(" ", "")
                                    .Replace("A", "")
                                    .Replace("B", "")
                                    .Replace("C", "")
                                    .Replace("D", "")
                                    //.Replace("E", "")
                                    .Replace("F", "")
                                    .Replace("G", "")
                                    .Replace("H", "")
                                    .Replace("I", "")
                                    .Replace("J", "")
                                    .Replace("K", "")
                                    .Replace("L", "")
                                    .Replace("M", "")
                                    .Replace("N", "")
                                    .Replace("O", "")
                                    .Replace("P", "")
                                    .Replace("Q", "")
                                    .Replace("R", "")
                                    .Replace("S", "")
                                    .Replace("T", "")
                                    .Replace("U", "")
                                    .Replace("V", "")
                                    .Replace("X", "")
                                    .Replace("Y", "")
                                    .Replace("Z", "");
            if (ss2.StartsWith("-"))
            {
                cc = "-" + cc;
            }
            return cc;
        }
        public static int Int(object comp)
        {
            string comps = comp.ToString();
            if (comps == "") { comps = "0"; }
            try
            {
                return Convert.ToInt32(Math.Ceiling(Double(comps.Replace(".", ","))));
            }
            catch (Exception)
            {

                return 0;
            }

        }
        public static bool Boolean(object obj)
        {
            try
            {
                return Convert.ToBoolean(obj);
            }
            catch (Exception)
            {

                return false;
            }
        }
    }
}
