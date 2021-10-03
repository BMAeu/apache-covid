import argparse
import apache_beam as beam
import apache_beam.runners.interactive.interactive_beam as ib
from apache_beam.runners.interactive.interactive_runner import InteractiveRunner

parser = argparse.ArgumentParser(description='Pega os paths para input e output')
parser.add_argument('--path_covid', dest = 'cov', help='the path to list')
parser.add_argument('--path_estado', dest = 'est', help='the path to list')
parser.add_argument('--path_output', dest = 'out', help='the path to list')
args = parser.parse_args()

pipeline = beam.Pipeline(InteractiveRunner())

# Lendo os csvs em Beam DataFrames
beam_estados = pipeline | 'Read estados' >> beam.dataframe.io.read_csv(args.est, delimiter = ';')
beam_covid = pipeline | 'Read covid' >> beam.dataframe.io.read_csv(args.cov, delimiter = ';')

# Transformando em DataFrame do pandas
covid = ib.collect(beam_covid)
estados = ib.collect(beam_estados)

# GroupBy para somar os casos e obitos, e renomeando as colunas para o DataFrame final
casos_e_obitos = covid.groupby(['coduf', 'regiao', 'estado'], as_index = False)['casosNovos', 'obitosNovos'].sum()
casos_e_obitos.rename(columns = {'casosNovos': 'TotalCasos', 'obitosNovos': 'TotalObitos'}, inplace = True)

# Fazendo um join dos dados de obitos e casos com os dados de estados usando os codigos de cada UF
df_final = casos_e_obitos.merge(estados[['UF [-]', 'Governador [2019]', 'Código [-]']], left_on = 'coduf', right_on = 'Código [-]')

df_final.rename(columns = {'regiao': 'Regiao', 'estado': 'UF', 'UF [-]': 'Estado', 'Governador [2019]': 'Governador'}, inplace = True)

df_final.to_csv(args.out + '\dados.csv', index = False, columns = ['Regiao', 'UF', 'Estado', 'Governador', 'TotalCasos', 'TotalObitos'])
df_final[['Regiao', 'UF', 'Estado', 'Governador', 'TotalCasos', 'TotalObitos']].to_json(args.out + '\dados.json', orient = 'records')



