# mvp-engenharia-dados-seguranca-rj

MVP Engenharia de Dados — Criminalidade no Estado do Rio de Janeiro

ALUNO: MATHEUS GABRIEL VIEIRA COIMBRA


Pipeline de dados construído no Databricks Free Edition, usando dados públicos do Instituto de Segurança Pública do Rio de Janeiro (ISP-RJ), para entender a evolução da criminalidade no estado, identificar concentrações geográficas de crime e investigar a relação entre atividade policial e indicadores de criminalidade.

[PREENCHER] Link do repositório GitHub / Databricks Repos: [PREENCHER] Link do workspace Databricks (se aplicável):

Contexto de Negócio e Perguntas (Etapa 2 e 4.1)
Problema

Entender como a criminalidade no Estado do Rio de Janeiro evoluiu ao longo de mais de duas décadas, identificar onde ela se concentra geograficamente e verificar se há indícios de que a atividade policial (prisões, apreensões, mandados cumpridos) está associada a variações subsequentes nos indicadores de crime.

Perguntas de negócio
1) Como o volume de crimes violentos evoluiu por região do estado (Capital, Baixada Fluminense, Grande Niterói, Interior) entre 2003 e 2026? (métrica composta: homicídio doloso, lesão corporal seguida de morte, latrocínio, morte por intervenção policial, tentativa de homicídio, lesão corporal dolosa e estupro — ver definição na seção de Modelagem)
2) Quais CISPs (delegacias) da Capital concentram a maior parte dos crimes patrimoniais (roubo de rua + furtos)? A distribuição é uniforme ou concentrada em poucas delegacias? Meses com mais atividade policial (prisões em flagrante + cumprimento de mandado de prisão) estão associados a variação nos crimes patrimoniais do mês seguinte?
3) O índice de recuperação de veículos está subindo, caindo ou estável ao longo dos anos e entre regiões?
4) Como feminicídio e tentativa de feminicídio evoluíram desde que passaram a ser registrados oficialmente (2015)? Há concentração em regiões específicas?
5) Existe sazonalidade mensal em roubo de rua e roubo a estabelecimento comercial ao longo do ano civil?

Nem todas as perguntas precisam ser respondidas com a mesma profundidade — a pergunta 3, em particular, é tratada como uma associação temporal exploratória, não como prova de causalidade (o nome "efetividade policial", usado numa versão inicial deste trabalho, foi deliberadamente evitado — ver ressalva completa na seção de Análise).

Fontes de dados e contexto

Todos os dados vêm do Instituto de Segurança Pública do Rio de Janeiro (ISP-RJ), autarquia vinculada à Secretaria de Estado de Segurança Pública, responsável por produzir e divulgar as estatísticas criminais oficiais do estado. Três arquivos foram utilizados:

Arquivo	Grão	Período coberto	Linhas	Descrição
BaseDPEvolucaoMensalCisp.csv	CISP (delegacia) × ano × mês	2003–2026	38.958	Contagens absolutas de ocorrências por delegacia (CISP) - https://www.ispdados.rj.gov.br/Arquivos/BaseDPEvolucaoMensalCisp.csv
BaseMunicipioMensal.csv	Município × ano × mês	2014–2026	13.892	Contagens absolutas de ocorrências por município - https://www.ispdados.rj.gov.br/Arquivos/BaseMunicipioMensal.csv
BaseMunicipioTaxaMes.csv	Município × ano × mês	2014–2024	12.144	Mesmas variáveis do arquivo anterior, mas como taxa por 100 mil habitantes/veículos - https://www.ispdados.rj.gov.br/Arquivos/BaseMunicipioTaxaMes.csv

Cada arquivo tem ~55–61 colunas, cobrindo desde crimes violentos (homicídio doloso, latrocínio, letalidade violenta), crimes de trânsito, roubos e furtos (por modalidade), crimes contra o patrimônio, até indicadores de atividade policial (prisões, apreensões, mandados cumpridos). O dicionário oficial de cada arquivo está anexado ao repositório (BaseDpDicionarioDeVariaveis.xlsx, BaseMunicípioMensalDicionarioDeVariaveis.xlsx, DicionarioDeVariaveisBaseMunicípioTaxaMês.xlsx).

Observação importante de cobertura: as três fontes não cobrem o mesmo período. A base de CISP vai de 2003 a 2026 (é a série histórica mais longa); a de município em contagem vai de 2014 a 2026; e a de município em taxa termina em dezembro de 2024 — provavelmente porque a taxa depende de estimativas populacionais do IBGE, que têm defasagem de publicação maior que os registros de ocorrência. Essa diferença de cobertura foi tratada explicitamente na Gold (ver seção de Qualidade de Dados).

Licença de uso

Os dados são publicados pelo ISP-RJ como dados abertos, no âmbito do Plano de Dados Abertos do Governo do Estado do Rio de Janeiro, com base na Lei de Acesso à Informação (Lei Federal nº 12.527/2011) e no Decreto Estadual nº 46.475/2018, que estabelecem o princípio da transparência ativa da administração pública. Os conjuntos de dados do ISP têm nível de acesso "Público" no catálogo oficial (dadosabertos.rj.gov.br) e estão disponíveis livremente no site do ISP (https://www.ispdados.rj.gov.br/) para uso por sociedade, pesquisadores e jornalistas. Não foi identificada uma licença Creative Commons explícita nos arquivos — o uso é amparado pelo caráter público e pela transparência ativa exigida por lei, mas recomenda-se citar o ISP-RJ como fonte em qualquer publicação derivada.


CARGA DOS DADOS (Etapa 4.2)

Os três arquivos CSV foram baixados diretamente do site do ISP-RJ e enviados para o volume do Unity Catalog do Databricks Free Edition 
caminho: 

  caminho_ocorrencias = "/Volumes/projeto_seguranca_rj/bronze/arquivos/BaseMunicipioMensal.csv"
  caminho_taxas = "/Volumes/projeto_seguranca_rj/bronze/arquivos/BaseMunicipioTaxaMes.csv"
  caminho_dp = "/Volumes/projeto_seguranca_rj/bronze/arquivos/BaseDPEvolucaoMensalCisp.csv"

A partir daí, cada arquivo foi lido via Notebook (PySpark) e persistido como tabela Delta na camada Bronze, sem qualquer transformação — apenas os dados como vieram, mais duas colunas de controle:

fonte_arquivo: nome do arquivo CSV de origem
data_ingestao: timestamp de quando a ingestão foi executada

Notebooks de ingestão Bronze: 01_bronze_seguranca_rj — referência no GitHub: [link].

Tabelas Bronze geradas:

projeto_seguranca_rj.bronze.dp_municipio
<img width="1347" height="646" alt="image" src="https://github.com/user-attachments/assets/212d62d9-7d74-4794-89dc-84ad2daff8dd" />

projeto_seguranca_rj.bronze.ocorrencias_municipio
<img width="1350" height="507" alt="image" src="https://github.com/user-attachments/assets/5dbcb3d9-d38b-4433-bfbf-05d6ec4c69e5" />

projeto_seguranca_rj.bronze.taxas_municipio
<img width="1342" height="489" alt="image" src="https://github.com/user-attachments/assets/1fb44515-52d7-49ca-b6b5-227b899def94" />



[PREENCHER screenshot]: print do Catalog Explorer mostrando as 3 tabelas Bronze persistidas, com contagem de linhas batendo com os CSVs originais (38.958 / 13.892 / 12.144).

Modelagem e Catálogo de Dados (Etapa 4.3)
Modelo escolhido

Foi adotada uma modelagem estrela simplificada dentro do Lakehouse: duas dimensões (tempo, município) e dois fatos, um por granularidade de análise (município e CISP).

dim_tempo                        (data_referencia, ano, mes, ano_mes, trimestre, semestre)
dim_municipio                    (fmun_cod, municipio, regiao)
fato_criminalidade_municipio     (grão: fmun_cod × ano × mes — 2014-2026)
fato_criminalidade_cisp          (grão: cisp × ano × mes — 2003-2026)

Decisão de modelagem: sem dim_cisp. Uma versão inicial deste trabalho tinha uma quarta tabela, dim_cisp, isolando os atributos de delegacia (cisp, aisp, risp, mcirc, municipio, regiao) numa dimensão própria. Essa tabela foi removida e os atributos passaram a viver diretamente dentro de fato_criminalidade_cisp. Duas razões:

Simplicidade proporcional ao escopo do MVP — com uma única fato usando esses atributos, uma dimensão separada só adiciona um join sem trazer benefício de normalização real.
Fidelidade histórica — se um CISP mudar de circunscrição municipal ao longo dos 23 anos de série, manter o atributo dentro da própria linha do fato preserva o valor como ele foi registrado naquele mês. Uma dimensão separada exigiria escolher uma única versão do atributo por CISP (ex.: a mais recente), o que reescreveria retroativamente o histórico.

Isso torna o modelo, tecnicamente, um híbrido estrela/flat — mais próximo do "Modelo Flat (por conceito)" citado no enunciado do trabalho para dados de Data Lake — e não um Esquema Estrela em sua forma pura. É uma simplificação deliberada e documentada, não um esquecimento.

Limitação de modelagem: fato_criminalidade_cisp não possui o código IBGE do município (fmun_cod), apenas o nome do município como texto — a base de CISP não traz esse código. Por isso, não há chave direta entre fato_criminalidade_cisp e dim_municipio; análises que cruzam as duas granularidades usam o campo regiao, que existe em ambas as fontes.

Catálogo de Dados
bronze.* (as 3 tabelas)

Réplica fiel dos CSVs originais + colunas de controle. Contexto, colunas, tipos e domínio de valores de cada uma das ~55-61 variáveis de indicadores criminais estão descritos nos dicionários oficiais do ISP-RJ, anexados ao repositório:

Tabela Bronze	Dicionário de referência
dp_municipio	BaseDpDicionarioDeVariaveis.xlsx
ocorrencias_municipio	BaseMunicípioMensalDicionarioDeVariaveis.xlsx
taxas_municipio	DicionarioDeVariaveisBaseMunicípioTaxaMês.xlsx

Grupos de variáveis presentes nas três tabelas (conforme dicionário oficial):

Crimes violentos: hom_doloso, lesao_corp_morte, latrocinio, cvli, hom_por_interv_policial, letalidade_violenta, tentat_hom, feminicidio, tentativa_feminicidio, lesao_corp_dolosa, estupro
Crimes de trânsito: hom_culposo, lesao_corp_culposa
Roubos: roubo_transeunte, roubo_celular, roubo_em_coletivo, roubo_rua, roubo_veiculo, roubo_carga, roubo_comercio, roubo_residencia, roubo_banco, roubo_cx_eletronico, roubo_conducao_saque, roubo_apos_saque, roubo_bicicleta, outros_roubos, total_roubos
Furtos: furto_veiculos, furto_transeunte, furto_coletivo, furto_celular, furto_bicicleta, outros_furtos, total_furtos
Outros crimes contra o patrimônio: sequestro, extorsao, sequestro_relampago, estelionato
Atividade policial: apreensao_drogas, posse_drogas, trafico_drogas, apreensao_drogas_sem_autor, recuperacao_veiculos, apf, aaapai, cmp, cmba
Outros registros: ameaca, pessoas_desaparecidas, encontro_cadaver, encontro_ossada, pol_militares_mortos_serv, pol_civis_mortos_serv
Registros de ocorrências: registro_ocorrencias (total consolidado)
Controle de versão da fonte: fase (2 = consolidado sem errata, 3 = consolidado com errata — ver Qualidade de Dados)

* feminicidio e tentativa_feminicidio só existem em dp_municipio e ocorrencias_municipio; a tabela de taxas não traz essas duas variáveis.

Nas tabelas dp_municipio e ocorrencias_municipio, todos os indicadores acima são contagens absolutas (tipo inteiro). Em taxas_municipio, os mesmos indicadores são taxas por 100 mil habitantes (ou por 100 mil veículos/policiais, conforme o campo — ver dicionário), tipo decimal.

silver.* (as 3 tabelas)

Mesma estrutura de colunas da Bronze, após tratamento de nulos, tipagem correta e remoção de duplicidade (ver Qualidade de Dados). Colunas-chave adicionadas/ajustadas:

Tabela Silver	Chave (grão)	Colunas de código convertidas para string
dp_municipio	cisp, ano, mes	cisp, aisp, risp, mcirc
ocorrencias_municipio	fmun_cod, ano, mes	fmun_cod
taxas_municipio	fmun_cod, ano, mes	fmun_cod
gold.* — Catálogo detalhado (tabelas criadas nesta etapa, sem dicionário externo)

dim_tempo

Coluna	Tipo	Descrição	Domínio
data_referencia	date	Primeiro dia do mês de referência	2003-01-01 a 2026-mm-01
ano	int	Ano de referência	2003–2026
mes	int	Mês de referência	1–12
mes_ano	string	Ano+mês no formato AAAAmMM	ex: 2014m01
trimestre	int	Trimestre do ano	1–4
semestre	int	Semestre do ano	1–2

Linhagem: união das combinações (ano, mês) de silver.dp_municipio e silver.ocorrencias_municipio, sem duplicidade.

dim_municipio

Coluna	Tipo	Descrição	Domínio
fmun_cod	string	Código IBGE de 7 dígitos do município	ex: 3304557
municipio	string	Nome do município	92 municípios do RJ
regiao	string	Região de segurança pública	Capital / Baixada Fluminense / Grande Niterói / Interior

Linhagem: silver.ocorrencias_municipio, mantendo a classificação de região mais recente por município (proteção contra eventual reclassificação ao longo do tempo).

fato_criminalidade_municipio (grão: fmun_cod × ano × mes)

Coluna	Tipo	Descrição
fmun_cod, fmun, regiao, ano, mes	string/int	Chave do grão + descritores herdados de silver.ocorrencias_municipio
hom_doloso, lesao_corp_morte, latrocinio, hom_por_interv_policial	int	Componentes atômicos de letalidade violenta
letalidade_violenta	int	Campo oficial do ISP-RJ (soma dos 4 componentes acima)
tentat_hom, lesao_corp_dolosa, estupro	int	Violência não-letal, somadas em crimes_violentos
crimes_violentos	int	Métrica derivada: hom_doloso + lesao_corp_morte + latrocinio + hom_por_interv_policial + tentat_hom + lesao_corp_dolosa + estupro. Usada na pergunta 1 (substitui letalidade_violenta como métrica principal, que fica disponível para comparação)
roubo_rua, roubo_comercio, roubo_veiculo, furto_veiculos, recuperacao_veiculos, total_furtos	int	Herdadas de silver.ocorrencias_municipio
feminicidio, tentativa_feminicidio	int	Usadas na pergunta 5
letalidade_violenta_taxa	double	Herdada de silver.taxas_municipio (única taxa mantida na Gold — as outras 52 foram descartadas por não serem usadas em nenhuma pergunta). Nula para 2025 e 2026 (fonte de taxas termina em 2024-12)
data_referencia	date	Derivada de ano + mes
veiculos_subtraidos	int	roubo_veiculo + furto_veiculos
indice_recuperacao_veiculos	double	Métrica derivada: recuperacao_veiculos / veiculos_subtraidos. Pode passar de 1 por efeito de defasagem (veículo furtado num mês, recuperado em mês posterior) — não é erro, é limitação documentada (ver Qualidade de Dados)

Linhagem: LEFT JOIN de silver.ocorrencias_municipio com silver.taxas_municipio por (fmun_cod, ano, mes).

fato_criminalidade_cisp (grão: cisp × ano × mes)

Coluna	Tipo	Descrição
cisp, aisp, risp, munic, mcirc, regiao, ano, mes	string/int	Chave do grão + atributos de delegacia herdados de silver.dp_municipio como registrados naquele mês (ver decisão de modelagem acima)
hom_doloso, lesao_corp_morte, latrocinio, hom_por_interv_policial	int	Componentes atômicos de letalidade violenta
letalidade_violenta	int	Campo oficial do ISP-RJ
tentat_hom, lesao_corp_dolosa, estupro	int	Violência não-letal
crimes_violentos	int	Métrica derivada (mesma fórmula da tabela municipal) — métrica principal da pergunta 1, por ser a única com série desde 2003
roubo_rua, total_furtos, apf, cmp	int	Herdadas de silver.dp_municipio
data_referencia	date	Derivada de ano + mes
atividade_policial	int	apf + cmp (prisões em flagrante + mandados de prisão cumpridos)
crimes_patrimoniais	int	roubo_rua + total_furtos
crimes_patrimoniais_mes_seguinte	int	crimes_patrimoniais do mês seguinte para o mesmo CISP, via LEAD sobre data_referencia. Só é preenchida quando o próximo registro é de fato +1 mês (checagem de continuidade via add_months) — evita comparar meses não consecutivos quando há lacuna na série. Usada na pergunta 3
variacao_crimes_mes_seguinte	int	crimes_patrimoniais_mes_seguinte - crimes_patrimoniais

Linhagem: silver.dp_municipio, sem join (apenas colunas derivadas por CISP/janela temporal), com a correção de encoding do campo regiao já herdada da Silver (ver Qualidade de Dados).

[PREENCHER screenshot]: prints do Catalog Explorer (ou DESCRIBE TABLE EXTENDED) de cada tabela Gold, evidenciando schema e linhagem no Unity Catalog.

Pipeline de Dados (Etapa 4.4)

O pipeline foi ramificado em notebooks separados por camada e por fonte, seguindo a Arquitetura Medalhão:

00_bronze_seguranca_rj_databricks.py    → lê os 3 CSVs (encoding ISO-8859-1) e grava em bronze.*
01_silver_seguranca_rj.ipynb            → bronze.dp_municipio          → silver.dp_municipio
02_silver_seguranca_rj.ipynb            → bronze.ocorrencias_municipio → silver.ocorrencias_municipio
03_silver_seguranca_rj.ipynb            → bronze.taxas_municipio       → silver.taxas_municipio
04_gold_seguranca_rj_databricks.py      → as 3 tabelas silver → 2 dimensões + 2 fatos em gold.*
05_analise_seguranca_rj_databricks.py   → qualidade de dados (Gold) + consultas das 6 perguntas

Os notebooks 00, 04 e 05 estão no formato nativo de notebook-fonte do Databricks (# Databricks notebook source / # COMMAND ----------); os 01–03 são .ipynb exportados diretamente do workspace. Ambos os formatos são reconhecidos pelo importador do Databricks — a escolha foi só uma questão de qual ferramenta gerou o arquivo primeiro.

Optou-se por um notebook por tabela/camada (em vez de um único notebook monolítico) para isolar responsabilidades: cada notebook Silver trata uma única fonte, o que facilita debugar problemas de qualidade específicos de cada arquivo (como ocorreu com a duplicidade e o encoding, ambos isolados a uma única fonte).

Principais transformações por notebook:

00 (Bronze): leitura dos 3 CSVs com delimiter=";" e encoding="ISO-8859-1" (usar UTF-8 aqui reproduziria o mesmo tipo de corrupção de acentuação corrigido na Silver), gravação em mode("overwrite") para manter a ingestão idempotente (os CSVs trazem o histórico completo a cada download, não são incrementais — append duplicaria tudo a cada execução), e validação de contagem de linhas contra o total esperado de cada arquivo.
01 (dp_municipio): tipagem de códigos (cisp, aisp, risp, mcirc) como string, tratamento de nulos por coalesce para 0 em 12 colunas (drogas, bicicleta, feminicídio, atividade policial — nulas antes da data de início de registro de cada indicador), remoção de duplicidade por revisão de fase, e correção do encoding do campo regiao (ver Qualidade de Dados para os dois problemas).
02 (ocorrencias_municipio): tipagem de fmun_cod como string, tratamento de nulos em feminicidio/tentativa_feminicidio, criação de data_referencia a partir de ano+mes.
03 (taxas_municipio): conversão de separador decimal (vírgula → ponto) e cast para double em 53 colunas de taxa, tratamento de 3 nulos em regiao.
04 (Gold): validação de schema (colunas obrigatórias — a execução para com erro se alguma faltar), validação de duplicidade de chave em cada tabela de origem e em cada fato gerado (também para a execução em caso de falha), checagem de consistência entre letalidade_violenta e seus 4 componentes documentados, construção das 2 dimensões e 2 fatos com as métricas derivadas (crimes_violentos, atividade_policial, crimes_patrimoniais, indice_recuperacao_veiculos), e limpeza de uma tabela dim_cisp de uma versão anterior do modelo (DROP TABLE IF EXISTS).
05 (Análise): perfil estatístico e outliers (IQR) das 4 métricas centrais da Gold, e as consultas/gráficos que respondem cada uma das 6 perguntas de negócio.

Referência aos scripts no GitHub: [PREENCHER links para cada notebook].

[PREENCHER screenshot]: print do Catalog Explorer mostrando as tabelas silver.* e gold.* persistidas (ou o output da célula final do notebook 04, que lista as 4 tabelas Gold com suas contagens de linha).

Qualidade de Dados (Etapa 4.5)

Três problemas reais de qualidade foram identificados e tratados ao longo do pipeline:

1. Duplicidade por revisão de dados (fase) — dp_municipio

A base BaseDPEvolucaoMensalCisp.csv publica, para os meses mais recentes, mais de um registro por (cisp, município, ano, mês): uma versão preliminar (fase = 2, "consolidado sem errata") e, depois, uma versão corrigida (fase = 3, "consolidado com errata"). Em alguns meses de 2026, a mesma versão preliminar (fase = 2) chegou a aparecer até 3 vezes (triplicata exata), provavelmente por reexportações do boletim que caíram no mesmo arquivo consolidado.

Tratamento: na Silver (01_silver_seguranca_rj), duplicatas exatas foram removidas com dropDuplicates(), e o conflito de versões (fase diferente para a mesma chave) foi resolvido mantendo sempre a fase mais alta (a versão corrigida/mais recente) por meio de uma janela (row_number() particionada por cisp, munic, ano, mes, ordenada por fase decrescente).

2. Encoding corrompido no campo regiao — dp_municipio

O valor "Grande Niterói" aparece corrompido em 2.610 das 2.890 ocorrências dessa categoria no arquivo bruto, como resultado de múltiplas recodificações de encoding (Grande NiterÃ\x83Â\x83Ã\x82Â\x83Ã\x83Â\x82Ã\x82Â³i). Sem tratamento, essa categoria seria contada como duas regiões diferentes, distorcendo qualquer agregação regional (pergunta 1).

Tratamento: normalização do campo regiao diretamente na Silver (01_silver_seguranca_rj) — qualquer valor contendo o radical "Niter" é padronizado para "Grande Niterói". A Gold também traz a mesma correção de forma defensiva (redundante, mas inofensiva) para o caso de a Silver ser executada numa versão anterior sem o fix. As demais fontes (ocorrencias_municipio, taxas_municipio) não apresentaram esse problema.

3. Cobertura temporal desigual entre as três fontes

taxas_municipio termina em dezembro de 2024, enquanto dp_municipio e ocorrencias_municipio vão até 2026. Isso não é um erro de coleta — é uma característica de publicação da fonte (taxas dependem de estimativa populacional do IBGE, publicada com mais atraso que os registros de ocorrência).

Tratamento: documentado e mantido como está — a única taxa mantida na Gold (letalidade_violenta_taxa) fica NULL para 2025/2026 em vez de ser preenchida artificialmente. Para a pergunta 4 (recuperação de veículos), uma métrica alternativa (indice_recuperacao_veiculos, calculada a partir das contagens) foi criada para cobrir todo o período sem depender da taxa oficial.

4. Consistência de indicadores compostos: letalidade_violenta vs. seus componentes

O dicionário do ISP-RJ documenta letalidade_violenta como a soma de 4 componentes (hom_doloso + lesao_corp_morte + latrocinio + hom_por_interv_policial). Essa composição foi conferida diretamente contra os dados brutos:

cvli (hom_doloso + lesao_corp_morte + latrocinio) bate perfeitamente em 100% das 38.958 linhas.
letalidade_violenta (cvli + hom_por_interv_policial) diverge em 215 linhas (0,55%), concentradas em outubro/novembro de 2024 com fase = 3 ("consolidado com errata").

Tratamento: documentado como uma inconsistência pontual da própria fonte (provavelmente ligada ao processo de correção/errata daqueles meses), não um bug do pipeline. A Gold traz uma célula de checagem que recalcula esse percentual a cada execução — se ele subir muito além de ~0,5%, vale investigar de novo antes de confiar nos números de letalidade daquele período.

5. Validações automáticas de schema e duplicidade (Gold)

Além das checagens pontuais acima, o notebook 04_gold valida, antes de persistir qualquer tabela:

Schema: todas as colunas obrigatórias de cada tabela Silver estão presentes — se faltar alguma, a execução para com ValueError em vez de silenciosamente gerar uma tabela incompleta.
Duplicidade de chave: em dp_municipio (antes de qualquer transformação) e em ambas as fatos geradas (fato_criminalidade_municipio, fato_criminalidade_cisp) — qualquer chave duplicada também interrompe a execução, em vez de só ser reportada.
6. Caso de acurácia: indice_recuperacao_veiculos acima de 1

Um veículo roubado/furtado em um mês pode ser recuperado só em um mês posterior — então o índice (recuperações do mês ÷ veículos subtraídos do mesmo mês) pode legitimamente passar de

Isso não é tratado como erro; a Silver/Gold não corrige nem limita esse valor.

[PREENCHER]: rode a célula de checagem no 05_analise (indice_recuperacao_veiculos > 1) e registre aqui o percentual de linhas afetadas, para dimensionar o efeito antes de discutir a pergunta 4.

Checagens de completude, consistência e unicidade

Além dos problemas acima, foram verificados sistematicamente em cada tabela Silver:

Completude: contagem de nulos por coluna via groupBy/agg(count(when(isNull))) em todas as ~55-61 colunas de cada tabela — os únicos nulos genuínos encontrados foram os já descritos (indicadores que só passaram a ser registrados a partir de certa data, e 3 nulos em regiao na base de taxas).
Unicidade: verificação de chave duplicada (cisp/fmun_cod + ano + mes) nas três tabelas — problema encontrado e corrigido apenas em dp_municipio (item 1 acima).
Consistência: verificação de que regiao só assume as 4 categorias esperadas — problema encontrado e corrigido em dp_municipio (item 2 acima).
Estatística descritiva e outliers (IQR) das métricas centrais

Perfil calculado em 05_analise sobre fato_criminalidade_cisp (grão CISP × mês), usando o método clássico de boxplot (outlier = fora de Q1 - 1,5×IQR e Q3 + 1,5×IQR):

Métrica	Média	Mediana	Q1	Q3	Limite inferior	Limite superior	Outliers	%
crimes_violentos	53,75	39	17	72	-65,5	154,5	1.883	4,92%
letalidade_violenta	3,63	2	0	5	-7,5	12,5	2.629	6,87%
crimes_patrimoniais	144,52	91	19	217	-278	514	1.310	3,42%
atividade_policial	24,42	17	5	35	-40	80	1.707	4,46%

Leitura dos resultados:

Todo limite inferior deu negativo — mas contagem de crime nunca é negativa, então não existe outlier "de baixo" em nenhuma das 4 métricas: todo outlier encontrado é CISP-mês com número acima do normal, nunca abaixo. Isso não é um problema de qualidade, é consequência esperada de trabalhar com dados de contagem (não-negativos) via um método pensado para distribuições simétricas.
Média sempre maior que a mediana nas 4 métricas — assinatura clássica de distribuição assimétrica à direita: a maioria dos CISP-meses tem números baixos, e um grupo pequeno de CISPs "puxa" a média para cima. Isso é evidência estatística a favor da pergunta 2 (concentração geográfica de crime em poucas delegacias), independente do ranking específico.
letalidade_violenta tem a maior % de outliers (6,87%), mas os valores absolutos são pequenos (mediana = 2, Q3 = 5) — homicídio é raro e esporádico, então um CISP tranquilo que teve um mês com poucas mortes a mais já estoura o limite estatístico. É um salto grande proporcionalmente, mas pequeno em número absoluto — não indica erro de dado.

[PREENCHER]: se quiser, cruze os outliers de crimes_patrimoniais com o ranking de CISPs da pergunta 2 — é esperado que os mesmos poucos CISPs concentrem a maior parte dos 1.310 outliers, o que reforçaria a resposta da pergunta 2 com evidência estatística, não só ranking.

Análise de Dados (Etapa 4.5)

Consultas e gráficos completos estão no notebook 05_analise_seguranca_rj_databricks.py. Resumo por pergunta (screenshots dos resultados devem ser colados abaixo de cada uma):

Pergunta 1 — Crimes violentos por região (2003–2026)

[PREENCHER screenshot do gráfico]

[PREENCHER discussão]: qual região tem a maior queda/alta proporcional desde o pico? Os valores são absolutos (não normalizados por população/número de CISPs) — comente essa limitação ao comparar regiões de tamanhos diferentes. A consulta também traz letalidade_violenta_total lado a lado — vale comentar se a tendência muda quando se olha só para os casos letais (mais graves) versus o conjunto mais amplo de violência.

Pergunta 2 — CISPs da Capital com maior concentração de crimes patrimoniais

[PREENCHER screenshot do gráfico]

[PREENCHER discussão]: qual percentual os 5 CISPs mais críticos representam do total da Capital? (o notebook já calcula esse número). Se cruzar com a análise de outliers da seção de Qualidade de Dados, comente se os CISPs do ranking são os mesmos que concentram os outliers de crimes_patrimoniais.

Pergunta 3 — Associação entre atividade policial e crimes patrimoniais do mês seguinte

[PREENCHER screenshot do gráfico e do valor de correlação]

Ressalva metodológica obrigatória para a discussão: tratada como associação temporal, não causalidade. Uma correlação positiva entre atividade policial e crime do mês seguinte pode significar o oposto do que parece à primeira vista — mais prisões tendem a ocorrer justamente onde já há mais criminalidade (causalidade reversa), não necessariamente que prender mais gera mais crime depois. Comente o resultado com essa ressalva, e compare o padrão entre regiões (o notebook já calcula a correlação por região separadamente). Note também que crimes_patrimoniais_mes_seguinte só é preenchida quando o próximo registro do CISP é realmente o mês seguinte (checagem de continuidade feita na Gold) — meses com lacuna na série ficam de fora do cálculo, em vez de comparar meses não consecutivos por engano.

Pergunta 4 — Evolução do índice de recuperação de veículos

[PREENCHER screenshot do gráfico]

[PREENCHER discussão]: o índice está subindo, caindo ou estável? Há diferença entre regiões? Mencione o percentual de linhas com índice acima de 1 (calculado na seção de Qualidade de Dados) como limitação metodológica — parte do índice reflete recuperações de veículos furtados em meses anteriores, não só do mês corrente.

Pergunta 5 — Feminicídio e tentativa de feminicídio desde 2015

[PREENCHER screenshot do gráfico e do ranking por região]

[PREENCHER discussão]: a série está subindo, caindo ou estável desde que passou a ser registrada? Qual região concentra mais casos?

Pergunta 6 — Sazonalidade mensal (roubo de rua vs. roubo a comércio)

[PREENCHER screenshot do gráfico]

[PREENCHER discussão]: existe pico em algum mês específico (ex.: dezembro)? O padrão é igual para os dois tipos de roubo?

Discussão geral

[PREENCHER]: conecte as 6 respostas de volta ao problema original — o que esse conjunto de achados diz, no geral, sobre a dinâmica da criminalidade no RJ e sobre a relação entre atividade policial e resultado de segurança pública?

Autoavaliação

Objetivos atingidos: [PREENCHER] — das 6 perguntas propostas, quais foram respondidas de forma satisfatória e quais ficaram incompletas ou exigiriam dados adicionais (ex.: população por município para normalizar comparações regionais na pergunta 1; nomes de delegacias para a pergunta 2).

Dificuldades encontradas: [PREENCHER] — por exemplo: identificar a causa da duplicidade em dp_municipio (exigiu investigar o significado do campo fase no dicionário de variáveis); descobrir o problema de encoding em regiao, que não aparecia em nenhuma checagem de nulos.

Trabalhos futuros:

Incorporar dados de população por município (IBGE) para normalizar comparações regionais na pergunta 1 sem o viés de tamanho populacional.
Buscar uma tabela de correspondência CISP → nome da delegacia para tornar a pergunta 2 mais legível.
Investigar a causalidade da pergunta 3 com métodos mais robustos (ex.: modelos de painel com efeitos fixos por CISP, para controlar o fato de que regiões mais violentas naturalmente têm mais atividade policial).
Investigar a origem da divergência de 0,55% entre letalidade_violenta e a soma dos seus 4 componentes documentados, concentrada em outubro/novembro de 2024 — possivelmente ligada ao processo de errata do ISP-RJ daquele período.
Automatizar a atualização mensal do pipeline conforme o ISP-RJ publica novos boletins.
