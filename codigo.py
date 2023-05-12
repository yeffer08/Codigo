import pandas as pd


# Especificar el tamaño del fragmento
chunksize = 7000
num_rows=sum(1 for line in open(('C:\\Users\\ale\\Documents\\storms.csv')))
print(num_rows)

# Calcular el número de fragmentos necesarios
num_chunks = (num_rows - 1) // chunksize + 1


# Leer y procesar cada fragmento
for i, df_chunk in enumerate(pd.read_csv('C:\\Users\\ale\\Documents\\storms.csv', delimiter="," ,chunksize=chunksize, iterator=True)):
    print(f'Leyendo fragmento {i+1} de {num_chunks}...')


       # Aquí va el procesamiento que desees aplicar a cada fragmento
    # ...

# Guardar el fragmento en un archivo separado
    df_chunk.to_csv(f'C:\\Users\\ale\\Documents\\sfragmento_{i+1}.csv', index=False)


# Unir todos los fragmentos en un solo DataFrame
dfs = []
for i in range(num_chunks):
    df_chunk = pd.read_csv(f'C:\\Users\\ale\\Documents\\sfragmento_{i+1}.csv')
    dfs.append(df_chunk)
df = pd.concat(dfs, ignore_index=True)

# Aplicar los filtros necesarios
# ...

# Guardar el DataFrame final en un archivo

df.to_csv('C:\\Users\\ale\\Documents\\archivo_final.csv', index=False)
final=(pd.read_csv('C:\\Users\\ale\\Documents\\archivo_final.csv', delimiter=","))
print(len(final))