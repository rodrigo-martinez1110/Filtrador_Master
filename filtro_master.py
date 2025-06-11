import streamlit as st
import pandas as pd
import re
from datetime import datetime
import io

# Configuração da página do Streamlit
st.set_page_config(page_title="Processador de Simulações", layout="wide")
st.title("Processador de Arquivos com Simulações")
pd.set_option("display.max_columns", None)

# Lista de colunas que esperamos ter no resultado final.
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

# --- Funções de Apoio ---

def encontrar_melhor_item(linha):
    """
    Percorre as simulações de uma linha e retorna a que tiver o maior número de parcelas.
    """
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
def processar_arquivos_simulacoes(files):
    """
    Função principal para processar os arquivos e extrair dados da coluna 'Simulacoes'.
    """
    if not files:
        return pd.DataFrame()

    lista_dfs = []
    progress = st.progress(0, text="Processando arquivos...")

    for i, file in enumerate(files):
        st.write(f"--- Processando arquivo: `{file.name}` ---")
        
        file.seek(0)
        content_bytes = file.read()
        
        try:
            primeira_linha = content_bytes.splitlines()[0].decode('latin1')
            sep = ';' if primeira_linha.count(';') > primeira_linha.count(',') else ','
            st.info(f"Separador detectado para '{file.name}': '{sep}'")
            
            file_buffer = io.BytesIO(content_bytes)
            df = pd.read_csv(file_buffer, sep=sep, encoding='latin1', low_memory=False, dtype=str)

        except Exception as e:
            st.error(f"Não foi possível ler o arquivo {file.name}. Erro: {e}")
            continue

        st.write("Colunas encontradas:", df.columns.tolist())
        st.dataframe(df.head(3))

        df['prazo_beneficio'] = pd.NA
        df['valor_liberado_beneficio'] = pd.NA
        df['valor_parcela_beneficio'] = pd.NA
        
        if 'Simulacoes' in df.columns:
            st.write("Coluna 'Simulacoes' encontrada. Extraindo a melhor oferta...")
            colunas_separadas = df['Simulacoes'].fillna('').astype(str).str.split('|', expand=True)
            colunas_separadas.columns = [f'Simulacoes_{j+1}' for j in range(colunas_separadas.shape[1])]
            
            df['Melhor_Item'] = colunas_separadas.apply(encontrar_melhor_item, axis=1)
            
            extracoes = df['Melhor_Item'].str.extract(r'(?P<prazo>\d+)x: (?P<valor>[\d.,]+) \(parcela: (?P<parcela>[\d.,]+)\)', expand=True)
            
            if not extracoes.empty:
                df['prazo_beneficio'] = pd.to_numeric(extracoes['prazo'], errors='coerce')
                
                # --- LÓGICA DE CONVERSÃO DE NÚMERO CORRIGIDA ---
                valor = extracoes['valor'].copy().astype(str)
                parcela = extracoes['parcela'].copy().astype(str)

                # Máscara para identificar formato PT-BR (que contém vírgula)
                mask_valor_br = valor.str.contains(',', na=False)
                mask_parcela_br = parcela.str.contains(',', na=False)

                # Para formato PT-BR: remove pontos de milhar e troca vírgula por ponto decimal
                valor[mask_valor_br] = valor[mask_valor_br].str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
                parcela[mask_parcela_br] = parcela[mask_parcela_br].str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
                
                # Converte para numérico. Formatos sem vírgula (ex: 1722.47) são convertidos diretamente.
                df['valor_liberado_beneficio'] = pd.to_numeric(valor, errors='coerce')
                df['valor_parcela_beneficio'] = pd.to_numeric(parcela, errors='coerce')
                
                st.success("Extração e conversão da coluna 'Simulacoes' concluída.")
            else:
                 st.warning("Não foi possível extrair dados do formato esperado na coluna 'Melhor_Item'.")
        else:
            st.warning(f"Coluna 'Simulacoes' não encontrada no arquivo {file.name}.")

        if 'CPF' in df.columns:
            df['CPF'] = df['CPF'].str.replace(r'\D', '', regex=True)
        if 'Nome_Cliente' in df.columns:
            df['Nome_Cliente'] = df['Nome_Cliente'].str.title()
            
        df = df.loc[df['valor_liberado_beneficio'].fillna(0) > 0]
        
        # --- FILTRO DE MARGEM CORRIGIDO ---
        if 'MG_Beneficio_Saque_Disponivel' in df.columns:
             df = df.loc[pd.to_numeric(df['MG_Beneficio_Saque_Disponivel'].fillna(0), errors='coerce') >= 0]
        
        st.write(f"Linhas restantes após filtros: {len(df)}")
        lista_dfs.append(df)
        progress.progress((i + 1) / len(files), text=f"Processando {file.name}...")

    if not lista_dfs:
        return pd.DataFrame()

    resultado_final = pd.concat(lista_dfs, ignore_index=True)
    st.success("Todos os arquivos foram processados!")
    return resultado_final

# --- Interface do Usuário (Sidebar) ---

st.sidebar.header("Uso do App")
st.sidebar.info(
    "1. Faça o upload de um ou mais arquivos CSV.\n"
    "2. O app buscará a coluna 'Simulacoes', extrairá a melhor oferta de saque.\n"
    "3. Defina os parâmetros de comissão e equipe.\n"
    "4. Baixe o resultado final."
)

st.sidebar.header("📂 Upload de Arquivos")
uploaded_files = st.sidebar.file_uploader(
    "Selecione os arquivos para processar",
    type="csv",
    accept_multiple_files=True
)

st.sidebar.header("⚙️ Parâmetros de Saída")
with st.sidebar.expander("Definir Parâmetros", expanded=True):
    equipes_konsi = ['outbound', 'csapp', 'csport', 'cscdx', 'csativacao', 'cscp']
    equipe = st.selectbox("Selecione a Equipe", equipes_konsi)
    comissao_banco = st.number_input("Comissão do banco (%)", value=10.0, step=0.5, min_value=0.0) / 100
    comissao_minima = st.number_input("Comissão mínima (R$)", value=50.0, step=10.0, min_value=0.0)

# --- Lógica Principal de Processamento ---

if uploaded_files:
    base_final = processar_arquivos_simulacoes(uploaded_files)

    if not base_final.empty:
        st.subheader("📊 Dados Processados")

        for col in colunas_finais:
            if col not in base_final.columns:
                base_final[col] = None

        base_final['banco_beneficio'] = '243'
        data_hoje = datetime.today().strftime('%d%m%Y')
        convenio_str = base_final['Convenio'].str.lower().fillna('geral') if 'Convenio' in base_final.columns else pd.Series(['geral'] * len(base_final))
        base_final['Campanha'] = convenio_str + '_' + data_hoje + '_benef_' + equipe

        base_final['valor_liberado_beneficio'] = pd.to_numeric(base_final['valor_liberado_beneficio'], errors='coerce').fillna(0)
        base_final['comissao_beneficio'] = (base_final['valor_liberado_beneficio'] * comissao_banco).round(2)
        base_final = base_final.query('comissao_beneficio >= @comissao_minima')

        base_final['MG_Emprestimo_Disponivel'] = 0

        colunas_existentes = [col for col in colunas_finais if col in base_final.columns]
        base_final = base_final[colunas_existentes]


        st.dataframe(base_final.head(1000))
        st.write(f"Total de registros no resultado final: **{base_final.shape[0]}**")
        st.write(f"Total de colunas: **{base_final.shape[1]}**")

        if not base_final.empty:
            csv = base_final.to_csv(sep=';', index=False, encoding='utf-8-sig').encode('utf-8-sig')
            nome_convenio = base_final["Convenio"].iloc[0] if pd.notna(base_final["Convenio"].iloc[0]) else "GERAL"
            
            st.download_button(
                label="📥 Baixar Resultado Final em CSV",
                data=csv,
                file_name=f'{nome_convenio}_BENEFICIO_{equipe.upper()}_{data_hoje}.csv',
                mime='text/csv'
            )
    else:
        st.warning("O processamento não resultou em dados válidos. Verifique o conteúdo dos arquivos e os filtros.")
else:
    st.info("Aguardando o upload de arquivos para iniciar o processamento.")
