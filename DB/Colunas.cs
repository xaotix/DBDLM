using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DB
{
    internal class Colunas
    {
        public override string ToString()
        {
            return $"{servidor} = {database}.{tabela} Colunas: {colunas.Count}x";
        }
        public List<string> colunas { get; set; } = new List<string>();
        public string database { get; set; } = "";
        public string servidor { get; set; } = "";
        public string tabela { get; set; } = "";
        public Colunas(string servidor, string database, string tabela, List<string> colunas)
        {
            this.servidor = servidor;
            this.database = database;
            this.tabela = tabela;
            this.colunas.AddRange(colunas);
        }
    }
}
