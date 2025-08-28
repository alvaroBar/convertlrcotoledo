# ==============================================================================
# ARQUIVO CORRIGIDO: converttoledo.py
# Corrigida a lógica de extração para processar múltiplos PDFs corretamente.
# ==============================================================================

import streamlit as st
import pdfplumber
import pandas as pd
import re
from io import BytesIO

# Importa as funções necessárias do nosso módulo loader
from bigquery_loader import autenticar_com_service_account, get_latest_week, autenticar_e_carregar


# --- Funções de Apoio ---

def processar_pdfs(lista_de_arquivos_pdf, disciplinas_validas, numero_da_semana):
    """
    Função principal que extrai os dados de uma lista de arquivos PDF.
    Corrigida para manter o contexto (escola, município) entre os arquivos.
    """
    dados_extraidos = []
    
    horario_re = r"\d{2}:\d{2}:\d{2}"
    registro_re = r"\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}"
    data_relatorio_re = r"\b\d{2}/\d{2}/\d{4}\b"

    # --- MUDANÇA CRÍTICA AQUI ---
    # Inicializa as variáveis de contexto FORA do loop para que persistam.
    nome_escola = "ESCOLA NÃO IDENTIFICADA"
    municipio = "MUNICÍPIO NÃO IDENTIFICADO"
    data_relatorio = "DATA NÃO IDENTIFICADA"

    for arquivo_pdf in lista_de_arquivos_pdf:
        turma_atual = None
        
        with pdfplumber.open(arquivo_pdf) as pdf:
            for page_num, page in enumerate(pdf.pages):
                texto_pagina = page.extract_text()
                if not texto_pagina: continue
                linhas = texto_pagina.split("\n")

                # A lógica de extração do cabeçalho agora ATUALIZA as variáveis
                # se encontrar novas informações, em vez de reiniciá-las.
                if page_num == 0:
                    for i, linha in enumerate(linhas):
                        if "ESTADO DO PARANÁ" in linha:
                            match_data = re.search(data_relatorio_re, linha)
                            if match_data: 
                                data_relatorio = match_data.group()
                        if "SECRETARIA DE ESTADO DA EDUCAÇÃO" in linha:
                            municipio_temp = linha.split("SECRETARIA")[0].strip()
                            if municipio_temp: # Só atualiza se encontrar um novo município
                                municipio = municipio_temp
                            if i + 1 < len(linhas):
                                nome_escola_temp = linhas[i + 1].strip()
                                if nome_escola_temp: # Só atualiza se encontrar uma nova escola
                                    nome_escola = nome_escola_temp
                
                # O restante da lógica de extração de linhas continua a mesma
                for linha in linhas:
                    linha = linha.strip()
                    # Lógica para identificar a linha que contém a "Turma"
                    if " - " in linha and "TURMA" not in linha and "LANÇAMENTO" not in linha:
                        turma_atual = linha
                        continue
                    if not turma_atual: 
                        continue

                    horarios = re.findall(horario_re, linha)
                    registros = re.findall(registro_re, linha)

                    if not horarios:
                        continue

                    horario = horarios[0]
                    pos_horario = linha.find(horario)
                    pos_fim_horario = pos_horario + len(horario)

                    registro_aula = registros[0] if len(registros) >= 1 else "Sem registro"
                    registro_conteudo = registros[1] if len(registros) >= 2 else "Sem registro"

                    pos_registro = linha.find(registros[0]) if registros else len(linha)
                    disciplina_raw = linha[pos_fim_horario:pos_registro].strip()

                    # Validação da disciplina
                    disciplina_encontrada = None
                    for nome_disciplina in disciplinas_validas:
                        if nome_disciplina in disciplina_raw.upper():
                            disciplina_encontrada = nome_disciplina
                            break

                    if not disciplina_encontrada:
                        continue  # pula linha se disciplina não reconhecida
                    
                    dados_extraidos.append([
                        numero_da_semana,
                        data_relatorio,
                        municipio,
                        nome_escola,
                        turma_atual,
                        horario,
                        disciplina_encontrada,
                        registro_aula,
                        registro_conteudo
                    ])

    colunas = [
        "SEMANA", "DATA_DO_RELATORIO", "MUNICIPIO", "ESCOLA", "TURMA",
        "HORARIO", "DISCIPLINA", "REGISTRO_DE_AULA", "REGISTRO_DE_CONTEUDO"
    ]
    df = pd.DataFrame(dados_extraidos, columns=colunas)
    return df


# --- Interface do Streamlit ---

st.set_page_config(layout="wide")
st.title("Conversor LRCO: PDF ➡️ BigQuery 📄➡️☁️")

# --- Lógica de Estado para persistir dados entre interações ---
if 'df_processado' not in st.session_state:
    st.session_state.df_processado = pd.DataFrame()

# --- Passo 1: Upload e Processamento ---
st.info("Passo 1: Carregue os arquivos PDF e a planilha de disciplinas.")
col1, col2 = st.columns(2)
with col1:
    uploaded_files = st.file_uploader("Selecione os arquivos PDF do relatório LRCO", type="pdf", accept_multiple_files=True)
with col2:
    disciplinas_file = st.file_uploader("Selecione a planilha com a lista oficial de disciplinas", type=["xlsx"])

if uploaded_files and disciplinas_file:
    if st.button("Processar Arquivos PDF"):
        try:
            disciplinas_df = pd.read_excel(disciplinas_file)
            lista_disciplinas_validas = [str(d).strip().upper() for d in disciplinas_df.iloc[:, 0].dropna().unique()]
            
            creds = autenticar_com_service_account()
            if creds:
                with st.spinner("Buscando última semana registrada no BigQuery..."):
                    ultima_semana = get_latest_week(creds)
                
                st.session_state.ultima_semana = ultima_semana
                st.session_state.semana_sugerida = ultima_semana + 1

                with st.spinner("Processando PDFs... Isso pode levar alguns momentos."):
                    df_temp = processar_pdfs(uploaded_files, lista_disciplinas_validas, 0)
                    st.session_state.df_processado = df_temp
            else:
                st.error("Não foi possível autenticar. Verifique as credenciais nos Segredos do Streamlit.")
        
        except Exception as e:
            st.error(f"Ocorreu um erro inesperado durante o processamento: {e}")

# --- Passo 2: Configuração e Envio ---
if not st.session_state.df_processado.empty:
    st.success(f"✅ Conversão concluída! {len(st.session_state.df_processado)} registros foram extraídos com sucesso.")
    
    st.markdown("---")
    st.subheader("Passo 2: Configure os dados para envio")
    
    # --- Seção de Configuração da Semana ---
    col_info, col_input = st.columns(2)
    with col_info:
        st.metric("Última Semana no Banco de Dados", st.session_state.get('ultima_semana', 'N/A'))
    
    with col_input:
        semana_para_envio = st.number_input(
            "Confirme ou altere o número da semana para estes novos registros:",
            min_value=1,
            value=st.session_state.get('semana_sugerida', 1),
            step=1
        )

    # --- Seção de Filtro de Disciplinas ---
    st.markdown("#### Filtrar Disciplinas")
    disciplinas_encontradas = sorted(st.session_state.df_processado['DISCIPLINA'].unique())
    
    disciplinas_selecionadas = st.multiselect(
        "Selecione as disciplinas que deseja enviar para o BigQuery (todas estão marcadas por padrão):",
        options=disciplinas_encontradas,
        default=disciplinas_encontradas
    )

    # Filtra o DataFrame com base na seleção do usuário
    df_filtrado = st.session_state.df_processado[st.session_state.df_processado['DISCIPLINA'].isin(disciplinas_selecionadas)]
    
    # Atualiza a coluna 'SEMANA' no DataFrame final
    df_para_envio = df_filtrado.copy()
    df_para_envio['SEMANA'] = semana_para_envio
    
    st.markdown("---")
    st.subheader("Passo 3: Envie os Dados")
    
    if not df_para_envio.empty:
        st.write(f"**{len(df_para_envio)}** registros prontos para serem enviados. Pré-visualização:")
        st.dataframe(df_para_envio.head())

        if st.button("Enviar para o BigQuery"):
            with st.spinner("Conectando e carregando dados..."):
                sucesso = autenticar_e_carregar(df_para_envio)
                if sucesso:
                    st.success(f"Dados da semana {semana_para_envio} enviados para o BigQuery com sucesso!")
                    st.balloons()
                    # Limpa o estado para um novo processamento
                    st.session_state.df_processado = pd.DataFrame()
                else:
                    st.error("Falha no envio dos dados. Verifique a mensagem de erro acima.")
    else:
        st.warning("Nenhuma disciplina foi selecionada. Nenhum dado será enviado.")
