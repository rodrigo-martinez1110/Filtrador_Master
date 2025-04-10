import streamlit as st
import pandas as pd
import re
from io import StringIO
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

def extrair_informacoes(item):
    if pd.notna(item):
        match = re.search(r'(\d+)x: ([\d.,]+) \(parcela: ([\d.,]+)\)', item)
        if match:
            parcelas = int(match.group(1))
            valor_liberado = float(match.group(2).replace(',', ''))
            valor_parcela = float(match.group(3).replace(',', ''))
            return parcelas, valor_liberado, valor_parcela
    return None, None, None

st.sidebar.subheader("📂 Arquivos Master")
uploaded_files = st.sidebar.file_uploader("Selecione os arquivos Master", type="csv", accept_multiple_files=True)

st.sidebar.write("---")

st.sidebar.subheader("📂 Arquivos com Margem")
arquivo_novo = st.sidebar.file_uploader("Arquivo com margem (opcional)", type="csv", accept_multiple_files=True)

st.sidebar.write("---")
st.sidebar.subheader("Filtros")
apenas_saque_complementar = st.sidebar.checkbox("Saldo Devedor maior que 0")

equipes_konsi = ['outbound', 'csapp', 'csport', 'cscdx', 'csativacao', 'cscp']
equipe = st.sidebar.selectbox("Selecione a Equipe", equipes_konsi)

base_final = pd.DataFrame()
convenio = "Arquivo"

# Processa base principal se enviada
if uploaded_files:
    lista = []
    for uploaded_file in uploaded_files:
        df = pd.read_csv(uploaded_file, sep=',', encoding='latin1', low_memory=False)
        lista.append(df)

    base = pd.concat(lista)
    convenio = base['Convenio'].iloc[0] if 'Convenio' in base.columns else 'Arquivo'

    colunas_separadas = base['Observacoes'].str.split('|', expand=True)
    colunas_separadas.columns = [f'Observacao_{i+1}' for i in range(colunas_separadas.shape[1])]
    base = pd.concat([base, colunas_separadas], axis=1)

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

    colunas_observacoes = [col for col in base.columns if col.startswith("Observacao_")]
    base['Melhor_Item'] = base[colunas_observacoes].apply(encontrar_melhor_item, axis=1)

    base = base[['Origem_Dado', 'Nome_Cliente', 'Matricula', 'CPF', 'Data_Nascimento',
                 'MG_Emprestimo_Total', 'MG_Emprestimo_Disponivel',
                 'MG_Beneficio_Saque_Total', 'MG_Beneficio_Saque_Disponivel',
                 'MG_Cartao_Total', 'MG_Cartao_Disponivel',
                 'Convenio', 'Vinculo_Servidor', 'Lotacao', 'Secretaria',
                 'Melhor_Item', 'Saldo_Devedor']]

    base[['prazo_beneficio', 'valor_liberado_beneficio', 'valor_parcela_beneficio']] = base['Melhor_Item'].apply(
        lambda x: pd.Series(extrair_informacoes(x))
    )

    base['prazo_beneficio'] = base['prazo_beneficio'].astype(str).str.replace(".0", "")
    base['CPF'] = base['CPF'].str.replace(r'\D', '', regex=True)
    base['Nome_Cliente'] = base['Nome_Cliente'].str.title()
    base = base.loc[~base['MG_Beneficio_Saque_Disponivel'].isna()]
    base = base.loc[base['valor_liberado_beneficio'] > 0]

    base_final = base[['Origem_Dado', 'Nome_Cliente', 'Matricula', 'CPF', 'Data_Nascimento',
                       'MG_Emprestimo_Total', 'MG_Emprestimo_Disponivel',
                       'MG_Beneficio_Saque_Total', 'MG_Beneficio_Saque_Disponivel',
                       'MG_Cartao_Total', 'MG_Cartao_Disponivel',
                       'Convenio', 'Vinculo_Servidor', 'Lotacao', 'Secretaria',
                       'valor_liberado_beneficio', 'valor_parcela_beneficio', 'prazo_beneficio', 'Saldo_Devedor']]

    if apenas_saque_complementar:
        base_final = base_final.loc[base_final['Saldo_Devedor'] > 0]

    st.success("Arquivo Master processado com sucesso!")

# Processa arquivo de margem separadamente se enviado
if arquivo_novo:
    valor_limite = st.sidebar.number_input("Valor Máximo de Margem", value=0.0)
    lista_novos = []
    for arq in arquivo_novo:
        df_novo = pd.read_csv(arq, sep=',', encoding='latin1', low_memory=False)
        df_novo['CPF'] = df_novo['CPF'].str.replace(r'\D', '', regex=True)
        df_novo = df_novo.loc[novo['MG_Emprestimo_Disponivel'] < valor_limite]
        colunas_para_merge = ['CPF', 'MG_Emprestimo_Total', 'MG_Emprestimo_Disponivel',
                          'Vinculo_Servidor', 'Lotacao', 'Secretaria']

        df_novo = df_novo.sort_values(by='MG_Emprestimo_Disponivel')
        df_novo = df_novo[colunas_para_merge].drop_duplicates(subset='CPF', ascending=False)
        
        lista_novos.append(df_novo)

    novo = pd.concat(lista_novos)

    

    csv_novo = novo.to_csv(sep=';', index=False).encode('utf-8')
    st.sidebar.download_button(
        label="📥 Baixar Arquivo de Margem (Novo)",
        data=csv_novo,
        file_name=f'{convenio} - MG EMPRESTIMO.csv',
        mime='text/csv'
    )

    # Faz merge se base principal estiver presente
    if not base_final.empty:
        base_final = base_final.merge(novo, on='CPF', how='left', suffixes=('', '_novo'))
        for col in ['MG_Emprestimo_Total', 'MG_Emprestimo_Disponivel', 'Vinculo_Servidor', 'Lotacao', 'Secretaria']:
            base_final[col] = base_final[f"{col}_novo"].combine_first(base_final[col])
            base_final.drop(columns=[f"{col}_novo"], inplace=True)

# Finalização e exportação
if not base_final.empty:
    for col in colunas_finais:
        if col not in base_final.columns:
            base_final[col] = None

    base_final = base_final[colunas_finais]
    data_hoje = datetime.today().strftime('%d%m%Y')
    base_final['Campanha'] = base_final['Convenio'].str.lower() + '_' + data_hoje + '_benef_' + equipe

    st.subheader("📊 Dados Processados")
    st.dataframe(base_final)
    st.write(base_final.shape)

    csv = base_final.to_csv(sep=';', index=False).encode('utf-8')
    st.download_button(
        label="📥 Baixar Resultado CSV",
        data=csv,
        file_name=f'{convenio} BENEFICIO - {equipe}.csv',
        mime='text/csv'
    )
else:
    st.info("Faça o upload de pelo menos um arquivo Master ou de Margem.")
