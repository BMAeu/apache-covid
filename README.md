# apache-covid

O código desenvolvido faz a leitura dos CSVs como Beam DataFrame, porém depois os transforma em DataFrames do Pandas, para realizar as operações necessárias, operações que não funcionaram quando aplicadas a um Beam DataFrame. Para a execução do código, basta rodar:

python hurb.py --path_covid {path para HIST_PAINEL_COVIDBR_28set2020.csv} --path_estado {path para EstadosIBGE.csv} --path_output {path para salvar o csv e o json}

Onde as {} devem ser substituídas pelos paths descritos. 
