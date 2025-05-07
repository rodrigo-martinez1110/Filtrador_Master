import streamlit as st
import pandas as pd
import re
from datetime import datetime

st.set_page_config(page_title="Processador de Arquivos Master", layout="wide")
st.title("Processador de Arquivos Master")
pd.set_option("display.max_columns", None)

colunas_finais = [
    'Origem_Dado', 'Nome_Cliente', 'Matricula', 'CPF', 'Data_Nascimento',
    'MG_Emprestimo_Total', 'MG_Emprestimo_Disponivel',
    'MG_Beneficio_Saque_Total', 'MG_Beneficio_Saque_Disponivel',
    'MG_Cartao_Total', 'MG_Cartao_Disponivel',
    'Convenio', 'Vinculo_Servidor', 'Lotacao', 'Secretaria',
    'FONE1', 'FONE2', 'FONE3', 'FONE4',
    'valor_liberado_emprestimo', 'valor_liberado_beneficio', 'valor_liberado_cartao',
    'comissao_emprestimo', 'comissao_beneficio', 'comissao_cartao',
    'valor_parcela_emprestimo', 'valor_parcela_beneficio', 'valor_parcela_cartao',
    'banco_emprestimo', 'banco_beneficio', 'banco_cartao',
    'prazo_emprestimo', 'prazo_beneficio', 'prazo_cartao',
    'Campanha'
]

def encontrar_melhor_item(linha):
    maior_parcela = 0
    melhor_item = None
    for item in linha:
        if pd.notna(item):
            match = re.search(r'(\d+)x:', str(item))
            if match:
                parcela = int(match.group(1))
                if parcela > maior_parcela:
                    maior_parcela = parcela
                    melhor_item = item
    return melhor_item

@st.cache_data
def processar_arquivos(files):
    resultado = []
    progress = st.progress(0)

    for i, file in enumerate(files):
        df = pd.read_csv(file, sep=',', encoding='latin1', low_memory=False)

        if 'Observacoes' in df.columns:
            colunas_separadas = df['Observacoes'].str.split('|', expand=True)
            colunas_separadas.columns = [f'Observacao_{i+1}' for i in range(colunas_separadas.shape[1])]
            df = pd.concat([df, colunas_separadas], axis=1)

            colunas_observacoes = [col for col in df.columns if col.startswith("Observacao_")]
            df['Melhor_Item'] = df[colunas_observacoes].apply(encontrar_melhor_item, axis=1)

            df['Melhor_Item'] = df['Melhor_Item'].fillna('')
            extracoes = df['Melhor_Item'].str.extract(r'(?P<prazo>\d+)x: (?P<valor>[\d.,]+) \(parcela: (?P<parcela>[\d.,]+)\)')

            df['prazo_beneficio'] = pd.to_numeric(extracoes['prazo'], errors='coerce')
            df['valor_liberado_beneficio'] = pd.to_numeric(extracoes['valor'].str.replace(',', ''), errors='coerce')
            df['valor_parcela_beneficio'] = pd.to_numeric(extracoes['parcela'].str.replace(',', ''), errors='coerce')

        df = df.loc[~df['MG_Beneficio_Saque_Disponivel'].isna()]
        df = df.loc[df['valor_liberado_beneficio'] > 0]

        df['CPF'] = df['CPF'].str.replace(r'\D', '', regex=True)
        df['Nome_Cliente'] = df['Nome_Cliente'].str.title()

        df = df[['Origem_Dado', 'Nome_Cliente', 'Matricula', 'CPF', 'Data_Nascimento',
                 'MG_Emprestimo_Total', 'MG_Emprestimo_Disponivel',
                 'MG_Beneficio_Saque_Total', 'MG_Beneficio_Saque_Disponivel',
                 'MG_Cartao_Total', 'MG_Cartao_Disponivel',
                 'Convenio', 'Vinculo_Servidor', 'Lotacao', 'Secretaria',
                 'valor_liberado_beneficio', 'valor_parcela_beneficio', 'prazo_beneficio', 'Saldo_Devedor']]

        resultado.append(df)
        progress.progress((i + 1) / len(files))

    return pd.concat(resultado, ignore_index=True)

# Sidebar
st.sidebar.subheader("📂 Arquivos Master")
uploaded_files = st.sidebar.file_uploader("Selecione os arquivos Master", type="csv", accept_multiple_files=True)

st.sidebar.subheader("📂 Arquivos com Margem")
arquivo_novo = st.sidebar.file_uploader("Arquivo com margem (opcional)", type="csv", accept_multiple_files=True)

st.sidebar.subheader("Filtros")
with st.sidebar.expander("Filtradores"):
    apenas_saque_complementar = st.checkbox("Saldo Devedor maior que 0")
    equipes_konsi = ['outbound', 'csapp', 'csport', 'cscdx', 'csativacao', 'cscp']
    equipe = selectbox("Selecione a Equipe", equipes_konsi)
    comissao_banco = number_input("Comissão do banco (%): ", value=0.00) / 100
    comissao_minima = number_input("Comissão mínima: ", value=0.0)
    

# Processamento principal
base_final = pd.DataFrame()

if uploaded_files:
    base_final = processar_arquivos(uploaded_files)
    if apenas_saque_complementar:
        base_final = base_final.loc[base_final['Saldo_Devedor'] > 0]
    st.success("Arquivo Master processado com sucesso!")

if arquivo_novo:
    valor_limite = st.sidebar.number_input("Valor Máximo de Margem Empréstimo", value=0.0)
    novos_resultados = []
    for arq in arquivo_novo:
        df_novo = pd.read_csv(arq, sep=',', encoding='latin1', low_memory=False)
        df_novo['CPF'] = df_novo['CPF'].str.replace(r'\D', '', regex=True)
        df_novo = df_novo.sort_values(by='MG_Emprestimo_Disponivel', ascending=False)
        df_novo = df_novo[['CPF', 'MG_Emprestimo_Total', 'MG_Emprestimo_Disponivel', 'Vinculo_Servidor', 'Lotacao', 'Secretaria']].drop_duplicates('CPF')
        novos_resultados.append(df_novo)

    novo = pd.concat(novos_resultados, ignore_index=True)

    csv_novo = novo.to_csv(sep=';', index=False).encode('utf-8')
    st.sidebar.download_button("📥 Baixar Arquivo de Margem (Novo)", data=csv_novo, file_name=f'MG_EMP_CSV.csv', mime='text/csv')

    if not base_final.empty:
        base_final = base_final.merge(novo, on='CPF', how='left', suffixes=('', '_novo'))
        for col in ['MG_Emprestimo_Total', 'MG_Emprestimo_Disponivel', 'Vinculo_Servidor', 'Lotacao', 'Secretaria']:
            base_final[col] = base_final[f"{col}_novo"].combine_first(base_final[col])
            base_final.drop(columns=[f"{col}_novo"], inplace=True)
        base_final = base_final.query('MG_Emprestimo_Disponivel <= @valor_limite')

if not base_final.empty:
    for col in colunas_finais:
        if col not in base_final.columns:
            base_final[col] = None

    base_final = base_final[colunas_finais]
    base['banco_beneficio'] = '243'
    data_hoje = datetime.today().strftime('%d%m%Y')
    base_final['Campanha'] = base_final['Convenio'].str.lower() + '_' + data_hoje + '_benef_' + equipe
    base_final['comissao_beneficio'] = (base_final['valor_liberado_beneficio'] * comissao_banco).round(2)

    
    base_final = base_final.query('comissao_beneficio >= @comissao_minima')

    base_final['MG_Emprestimo_Disponivel'] = 0

    st.subheader("📊 Dados Processados")
    st.dataframe(base_final.head(1000))
    st.write(base_final.shape)

    csv = base_final.to_csv(sep=';', index=False).encode('utf-8')
    st.download_button("📥 Baixar Resultado CSV", data=csv, file_name=f'{base_final["Convenio"].iloc[0]}_BENEFICIO_{equipe}.csv', mime='text/csv')
else:
    st.info("Faça o upload de pelo menos um arquivo Master ou de Margem.")
