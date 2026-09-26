# mvp-engenharia-dados-seguranca-rj

MVP Engenharia de Dados — Criminalidade no Estado do Rio de Janeiro

Sprint: Engenharia de Dados (40530010057_20260_01)

ALUNO: MATHEUS GABRIEL VIEIRA COIMBRA

github: https://github.com/MatheusVieira12

Pipeline de dados construído no Databricks Free Edition, usando dados públicos do Instituto de Segurança Pública do Rio de Janeiro (ISP-RJ), para entender a evolução da criminalidade no estado, identificar concentrações geográficas de crime e investigar a relação entre atividade policial e indicadores de criminalidade.

*Repositório do Projeto*
https://github.com/MatheusVieira12/mvp-engenharia-dados-seguranca-rj/edit/main/README.md 

Contexto de Negócio e Perguntas (Etapa 2 e 4.1)
Problema

Entender como a criminalidade no Estado do Rio de Janeiro evoluiu ao longo de mais de duas décadas, identificar onde ela se concentra geograficamente e verificar se há indícios de que a atividade policial (prisões, apreensões, mandados cumpridos) está associada a variações subsequentes nos indicadores de crime.

### Perguntas de negócio

1. Como o volume de crimes violentos evoluiu por região do Estado do Rio de Janeiro entre 2003 e 2026?

2. Quais CISPs da Capital concentram os maiores volumes de crimes patrimoniais, considerando roubo de rua e furtos?

3. Existe associação entre a atividade policial de um mês, medida por prisões em flagrante e cumprimento de mandados de prisão, e a quantidade de crimes patrimoniais registrada no mês seguinte?

4. Como o índice de recuperação de veículos evoluiu ao longo dos anos e entre as regiões?

5. Como evoluíram mensalmente os registros de feminicídio e tentativa de feminicídio a partir de outubro de 2024?

6. Quais meses apresentam os maiores registros de roubo de rua e roubo a estabelecimento comercial em cada ano, e esses meses de maior ocorrência se repetem ao longo dos anos?

Nem todas as perguntas precisam ser respondidas com a mesma profundidade — a pergunta 3, em particular, é tratada como uma associação temporal exploratória, não como prova de causalidade (o nome "efetividade policial", usado numa versão inicial deste trabalho, foi deliberadamente evitado — ver ressalva completa na seção de Análise).


### FONTES DE DADOS E CONTEXTO


Todos os dados vêm do Instituto de Segurança Pública do Rio de Janeiro (ISP-RJ), autarquia vinculada à Secretaria de Estado de Segurança Pública, responsável por produzir e divulgar as estatísticas criminais oficiais do estado. Três arquivos foram utilizados:

Arquivo	Grão	Período coberto	Linhas	Descrição
BaseDPEvolucaoMensalCisp.csv	CISP (delegacia) × ano × mês	2003–2026	38.958	Contagens absolutas de ocorrências por delegacia (CISP) - https://www.ispdados.rj.gov.br/Arquivos/BaseDPEvolucaoMensalCisp.csv
BaseMunicipioMensal.csv	Município × ano × mês	2014–2026	13.892	Contagens absolutas de ocorrências por município - https://www.ispdados.rj.gov.br/Arquivos/BaseMunicipioMensal.csv
BaseMunicipioTaxaMes.csv	Município × ano × mês	2014–2024	12.144	Mesmas variáveis do arquivo anterior, mas como taxa por 100 mil habitantes/veículos - https://www.ispdados.rj.gov.br/Arquivos/BaseMunicipioTaxaMes.csv

Cada arquivo tem ~55–61 colunas, cobrindo desde crimes violentos (homicídio doloso, latrocínio, letalidade violenta), crimes de trânsito, roubos e furtos (por modalidade), crimes contra o patrimônio, até indicadores de atividade policial (prisões, apreensões, mandados cumpridos). O dicionário oficial de cada arquivo está anexado ao repositório (BaseDpDicionarioDeVariaveis.xlsx, BaseMunicípioMensalDicionarioDeVariaveis.xlsx, DicionarioDeVariaveisBaseMunicípioTaxaMês.xlsx).

Observação importante de cobertura: as três fontes não cobrem o mesmo período. A base por CISP possui registros de 2003 a 2026, a base municipal de contagens cobre 2014 a 2026 e a base municipal de taxas possui dados até dezembro de 2024. Essa diferença de cobertura foi preservada no pipeline e considerada nas análises, sem preenchimento artificial dos períodos sem dados.

### LICENÇA DE USO

Os dados são publicados pelo ISP-RJ como dados abertos, no âmbito do Plano de Dados Abertos do Governo do Estado do Rio de Janeiro, com base na Lei de Acesso à Informação (Lei Federal nº 12.527/2011) e no Decreto Estadual nº 46.475/2018, que estabelecem o princípio da transparência ativa da administração pública. Os conjuntos de dados do ISP têm nível de acesso "Público" no catálogo oficial (dadosabertos.rj.gov.br) e estão disponíveis livremente no site do ISP (https://www.ispdados.rj.gov.br/) para uso por sociedade, pesquisadores e jornalistas. Não foi identificada uma licença Creative Commons explícita nos arquivos — o uso é amparado pelo caráter público e pela transparência ativa exigida por lei, mas recomenda-se citar o ISP-RJ como fonte em qualquer publicação derivada.


### CARGA DOS DADOS (Etapa 4.2)


Os três arquivos CSV foram baixados diretamente do site do ISP-RJ e enviados para o volume do Unity Catalog do Databricks Free Edition 
caminho: 

  caminho_ocorrencias = "/Volumes/projeto_seguranca_rj/bronze/arquivos/BaseMunicipioMensal.csv"
  caminho_taxas = "/Volumes/projeto_seguranca_rj/bronze/arquivos/BaseMunicipioTaxaMes.csv"
  caminho_dp = "/Volumes/projeto_seguranca_rj/bronze/arquivos/BaseDPEvolucaoMensalCisp.csv"

A partir daí, cada arquivo foi lido via Notebook (PySpark) e persistido como tabela Delta na camada Bronze, sem qualquer transformação — apenas os dados como vieram, mais duas colunas de controle:

fonte_arquivo: nome do arquivo CSV de origem
data_ingestao: timestamp de quando a ingestão foi executada

Notebook de ingestão Bronze:  [`01_bronze_seguranca_rj.ipynb`](notebooks/01_bronze_seguranca_rj.ipynb)

Tabelas Bronze geradas:

projeto_seguranca_rj.bronze.dp_municipio
<img width="1347" height="646" alt="image" src="https://github.com/user-attachments/assets/212d62d9-7d74-4794-89dc-84ad2daff8dd" />

projeto_seguranca_rj.bronze.ocorrencias_municipio
<img width="1350" height="507" alt="image" src="https://github.com/user-attachments/assets/5dbcb3d9-d38b-4433-bfbf-05d6ec4c69e5" />

projeto_seguranca_rj.bronze.taxas_municipio
<img width="1342" height="489" alt="image" src="https://github.com/user-attachments/assets/1fb44515-52d7-49ca-b6b5-227b899def94" />


### MODELAGEM DE DADOS (Etapa 4.3)

Modelo escolhido: Foi adotada uma modelagem estrela simplificada dentro do Lakehouse: duas dimensões (tempo, município) e dois fatos, um por granularidade de análise (município e CISP).

dim_tempo                        (data_referencia, ano, mes, ano_mes, trimestre, semestre)
dim_municipio                    (fmun_cod, municipio, regiao)
fato_criminalidade_municipio     (grão: fmun_cod × ano × mes — 2014-2026)
fato_criminalidade_cisp          (grão: cisp × ano × mes — 2003-2026)

Decisão de modelagem: sem dim_cisp. Uma versão inicial deste trabalho tinha uma quarta tabela, dim_cisp, isolando os atributos de delegacia (cisp, aisp, risp, mcirc, municipio, regiao) numa dimensão própria. Essa tabela foi removida e os atributos passaram a viver diretamente dentro de fato_criminalidade_cisp. Duas razões:

Simplicidade proporcional ao escopo do MVP — com uma única fato usando esses atributos, uma dimensão separada só adiciona um join sem trazer benefício de normalização real.
Fidelidade histórica — se um CISP mudar de circunscrição municipal ao longo dos 23 anos de série, manter o atributo dentro da própria linha do fato preserva o valor como ele foi registrado naquele mês. Uma dimensão separada exigiria escolher uma única versão do atributo por CISP (ex.: a mais recente), o que reescreveria retroativamente o histórico.

Isso torna o modelo, tecnicamente, um híbrido estrela/flat — mais próximo do "Modelo Flat (por conceito)" citado no enunciado do trabalho para dados de Data Lake — e não um Esquema Estrela em sua forma pura. É uma simplificação deliberada e documentada.

Limitação de modelagem: fato_criminalidade_cisp não possui o código IBGE do município (fmun_cod), apenas o nome do município como texto — a base de CISP não traz esse código. Por isso, não há chave direta entre fato_criminalidade_cisp e dim_municipio; análises que cruzam as duas granularidades usam o campo regiao, que existe em ambas as fontes.

### CATÁLOGO DE DADOS

bronze.* (as 3 tabelas)

Réplica fiel dos CSVs originais + colunas de controle. Contexto, colunas, tipos e domínio de valores de cada uma das ~55-61 variáveis de indicadores criminais estão descritos nos dicionários oficiais do ISP-RJ, anexados ao repositório:

Tabela Bronze	Dicionário de referência

dp_municipio	          BaseDpDicionarioDeVariaveis.xlsx
ocorrencias_municipio	  BaseMunicípioMensalDicionarioDeVariaveis.xlsx
taxas_municipio	        DicionarioDeVariaveisBaseMunicípioTaxaMês.xlsx

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

Coluna	    Tipo	    Descrição	Domínio
fmun_cod	  string	  Código IBGE de 7 dígitos do município	ex: 3304557
municipio	  string	  Nome do município	92 municípios do RJ
regiao	    string	  Região de segurança pública	Capital / Baixada Fluminense / Grande Niterói / Interior

Linhagem: silver.ocorrencias_municipio, mantendo a classificação de região mais recente por município (proteção contra eventual reclassificação ao longo do tempo).

fato_criminalidade_municipio (grão: fmun_cod × ano × mes)

Coluna	                                                          Tipo	                          Descrição
fmun_cod, fmun, regiao, ano, mes	                              string/int	                      Chave do grão + descritores herdados de silver.ocorrencias_municipio
hom_doloso, lesao_corp_morte, latrocinio, hom_por_interv_policial	int	                            Componentes atômicos de letalidade violenta
letalidade_violenta	                                              int	                            Campo oficial do ISP-RJ (soma dos 4 componentes acima)
tentat_hom, lesao_corp_dolosa, estupro	                          int	                                                          Violência não-letal, somadas em crimes_violentos
crimes_violentos	                                                                                 int	Métrica derivada: hom_doloso + lesao_corp_morte + latrocinio + hom_por_interv_policial + tentat_hom + lesao_corp_dolosa + estupro. Usada na pergunta                                                                                                       1 (substitui letalidade_violenta como métrica principal, que fica disponível para comparação)
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

 <img width="193" height="397" alt="image" src="https://github.com/user-attachments/assets/570e6ab3-0937-4050-b933-9357d1d4f692" />


### PIPELINE DE DADOS (Etapa 4.4)

O pipeline foi ramificado em notebooks separados por camada e por fonte, seguindo a Arquitetura Medalhão:

setup_crime.ipynb                    → preparação do ambiente e criação dos schemas bronze, silver e gold
01_bronze_seguranca_rj.ipynb         → leitura dos 3 CSVs e persistência em bronze.*
01_silver_seguranca_rj.ipynb         → bronze.dp_municipio → silver.dp_municipio
02_silver_seguranca_rj.ipynb         → bronze.ocorrencias_municipio → silver.ocorrencias_municipio
03_silver_seguranca_rj.ipynb         → bronze.taxas_municipio → silver.taxas_municipio
04_gold_seguranca_rj.py              → 3 tabelas Silver → 2 dimensões + 2 fatos
05_analise_seguranca_rj.py           → qualidade + respostas às 6 perguntas


Optou-se por um notebook por tabela/camada (em vez de um único notebook monolítico) para isolar responsabilidades: cada notebook Silver trata uma única fonte, o que facilita debugar problemas de qualidade específicos de cada arquivo (como ocorreu com a duplicidade e o encoding, ambos isolados a uma única fonte).

Principais transformações por notebook:

00 (Bronze): leitura dos 3 CSVs com delimiter=";" e encoding= " latin1 " (usar UTF-8 aqui reproduziria o mesmo tipo de corrupção de acentuação corrigido na Silver), gravação em mode("overwrite") para manter a ingestão idempotente (os CSVs trazem o histórico completo a cada download, não são incrementais — append duplicaria tudo a cada execução), e validação de contagem de linhas contra o total esperado de cada arquivo.
01 (dp_municipio): tipagem de códigos (cisp, aisp, risp, mcirc) como string, tratamento de nulos por coalesce para 0 em 12 colunas (drogas, bicicleta, feminicídio, atividade policial — nulas antes da data de início de registro de cada indicador), remoção de duplicidade por revisão de fase, e correção do encoding do campo regiao (ver Qualidade de Dados para os dois problemas).
02 (ocorrencias_municipio): tipagem de fmun_cod como string, tratamento de nulos em feminicidio/tentativa_feminicidio, criação de data_referencia a partir de ano+mes.
03 (taxas_municipio): conversão de separador decimal (vírgula → ponto) e cast para double em 53 colunas de taxa, tratamento de 3 nulos em regiao.
04 (Gold): validação de schema (colunas obrigatórias — a execução para com erro se alguma faltar), validação de duplicidade de chave em cada tabela de origem e em cada fato gerado (também para a execução em caso de falha), checagem de consistência entre letalidade_violenta e seus 4 componentes documentados, construção das 2 dimensões e 2 fatos com as métricas derivadas (crimes_violentos, atividade_policial, crimes_patrimoniais, indice_recuperacao_veiculos).
05 (Análise): perfil estatístico e identificação de possíveis outliers pelo método IQR nas principais métricas utilizadas nas análises municipais e por CISP, além das consultas e visualizações utilizadas para responder às 6 perguntas de negócio.

Referência aos scripts no GitHub: [PREENCHER links para cada notebook].

[PREENCHER screenshot]: print do Catalog Explorer mostrando as tabelas silver.* e gold.* persistidas (ou o output da célula final do notebook 04, que lista as 4 tabelas Gold com suas contagens de linha).

### QUALIDADE DOS DADOS (Etapa 4.5)

Ao longo do pipeline foram identificados problemas de qualidade, diferenças de cobertura e situações que exigiram tratamento ou validação específica:

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

7 Tratamento de valores nulos — feminicídio e tentativa de feminicídio

Nas bases dp_municipio e ocorrencias_municipio, os campos feminicidio e tentativa_feminicidio apresentavam valores nulos em períodos anteriores à disponibilidade desses indicadores.

Na camada Silver, esses valores foram tratados com coalesce, substituindo NULL por 0. Essa transformação evita problemas em operações posteriores de soma e agregação.

Entretanto, os zeros anteriores ao período válido não são interpretados como ausência de ocorrências. Por esse motivo, a análise desses indicadores neste MVP considera somente os registros a partir de outubro de 2024.

8 Tratamento das taxas — separador decimal e conversão para double

Na tabela taxas_municipio, os valores numéricos foram disponibilizados utilizando vírgula como separador decimal, por exemplo "0,55".

Para permitir operações matemáticas no Spark, a vírgula foi substituída por ponto e as 53 colunas de taxas foram convertidas para o tipo double.

Exemplo:

"0,55" → 0.55

Esse tratamento permite realizar corretamente médias, comparações, ordenações e outras operações numéricas.

### Valores nulos em `regiao` — taxas_municipio

Foram identificados 3 registros com valor nulo no campo regiao da tabela taxas_municipio.

Esses valores foram preenchidos com NAO_INFORMADO. Dessa forma, os registros foram preservados sem atribuir artificialmente uma região que não estava disponível na fonte.

### Tipagem dos códigos identificadores

Campos utilizados como códigos, como cisp, aisp, risp, mcirc e fmun_cod, foram convertidos para string.

Apesar de serem compostos por números, esses campos funcionam como identificadores e não representam quantidades destinadas a cálculos matemáticos.

9) Criação da data de referência mensal

As bases utilizadas possuem granularidade mensal e apresentam os campos `ano` e `mes`, sem informação de dia.

Para permitir o uso de funções temporais do Spark, foi criada a coluna `data_referencia` utilizando o primeiro dia de cada mês como data convencional de referência.

Exemplo:

`ano = 2024` e `mes = 10` → `data_referencia = 2024-10-01`

O valor `01` não representa o dia em que a ocorrência aconteceu. Ele é utilizado apenas para transformar a combinação de ano e mês em uma data válida.

Essa padronização facilita a ordenação cronológica, criação da dimensão de tempo, geração de gráficos de série temporal e operações como identificação do mês seguinte.


### CHECAGENS DE COMPLETUDE, CONSITÊNCIA e UNICIDADE


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

### ANÁLISE DE DADOS (Etapa 4.5)

Consultas e gráficos completos estão no notebook 05_analise_seguranca_rj_databricks.py. Resumo por pergunta (screenshots dos resultados devem ser colados abaixo de cada uma):

### Pergunta 1 — Como o volume de crimes violentos evoluiu por região do Estado do Rio de Janeiro entre 2003 e 2026?

<img width="1102" height="504" alt="image" src="https://github.com/user-attachments/assets/7faf3e9a-a6bc-4dba-969e-f59108f39b1d" />

A análise mostra que a evolução dos registros de crimes violentos não ocorreu da mesma forma em todas as regiões do Estado do Rio de Janeiro. Cada região atingiu seu maior volume em um momento diferente da série histórica. A Capital apresentou o maior pico absoluto, com 40.899 registros em 2013. No Interior, o maior valor foi observado em 2014, com 29.545 registros. A Baixada Fluminense alcançou seu pico em 2012, com 26.032 registros, enquanto Grande Niterói apresentou seu maior volume em 2014, com 9.387 registros. Esses resultados mostram que a concentração dos registros varia tanto entre as regiões quanto ao longo do tempo. A Capital apresentou o maior volume absoluto entre as quatro regiões, mas essa comparação deve ser feita com cautela, pois os valores utilizados são contagens absolutas e não foram normalizados pela população ou pela quantidade de CISPs de cada região. Também foi verificada a cobertura temporal dos dados. Os anos anteriores possuem os 12 meses disponíveis, enquanto 2026 apresenta registros somente até agosto. Por isso, o total de 2026 não deve ser comparado diretamente com os anos completos como evidência de aumento ou redução da criminalidade. Assim, a análise permite identificar os principais picos históricos e as diferenças regionais na evolução dos registros, mas comparações proporcionais entre as regiões exigiriam indicadores normalizados, como taxas por população.

<img width="241" height="87" alt="image" src="https://github.com/user-attachments/assets/a210f1c1-7d27-4b2c-a8b1-0ff57236dc52" />


### Pergunta 2 — Quais CISPs da Capital concentram os maiores volumes de crimes patrimoniais, considerando roubo de rua e furtos?

<img width="664" height="393" alt="image" src="https://github.com/user-attachments/assets/b6666d6f-384b-4311-aa15-eafedc4e0843" />

A análise dos registros de crimes patrimoniais da Capital, considerando a soma de `roubo_rua` e `total_furtos`, mostrou que os maiores volumes estão concentrados em algumas CISPs específicas. No período analisado, as cinco CISPs com maior volume foram a **CISP 5 (Centro/Lapa)**, **CISP 16 (Barra da Tijuca)**, **CISP 35 (Campo Grande)**, **CISP 34 (Bangu)** e **CISP 12 (Copacabana/Leme)**. Entre elas, a CISP 5 apresentou o maior volume acumulado de registros.

As cinco CISPs com maior volume concentraram **22,1% de todos os registros de crimes patrimoniais da Capital**. Esse resultado mostra que existe concentração em determinadas circunscrições, embora ela não esteja limitada apenas a um pequeno grupo, já que aproximadamente 77,9% dos registros estão distribuídos entre as demais CISPs. A análise utiliza valores absolutos acumulados, portanto os resultados indicam onde houve maior quantidade de registros, mas não significam necessariamente maior risco proporcional. Fatores como população residente, circulação diária de pessoas, atividade comercial e extensão territorial das CISPs não foram considerados nessa comparação.


### Pergunta 3 — Existe associação entre a atividade policial de um mês e a quantidade de crimes patrimoniais registrada no mês seguinte?

<img width="560" height="404" alt="image" src="https://github.com/user-attachments/assets/4391b002-7ddb-443e-ab69-7e42e0985924" />

A análise comparou a atividade policial registrada em cada CISP, medida pela soma de prisões em flagrante (apf) e cumprimento de mandados de prisão (cmp), com a quantidade de crimes patrimoniais registrada no mês seguinte.
A correlação geral encontrada foi de 0,528, indicando uma associação positiva de magnitude moderada entre as duas variáveis. Isso significa que, nos dados analisados, meses com maior atividade policial tendem a estar associados a maiores volumes de crimes patrimoniais no mês seguinte. Quando a análise foi separada por região, os valores encontrados foram:

- Baixada Fluminense: 0,654
- Interior: 0,638
- Grande Niterói: 0,555
- Capital: 0,450

A associação foi positiva em todas as regiões. A Baixada Fluminense apresentou a maior correlação, seguida pelo Interior e por Grande Niterói. A Capital apresentou o menor valor entre as quatro regiões. Mesmo assim, esse resultado não deve ser interpretado como uma relação de causa e efeito. A correlação mostra apenas que as duas variáveis variam juntas em certa medida. Regiões com maiores níveis de criminalidade também podem apresentar maior atuação policial, o que pode contribuir para essa associação observada. Além disso, a análise considerou somente os casos em que o registro seguinte correspondia realmente ao mês seguinte para o mesmo CISP, evitando comparações incorretas em períodos com lacunas na série.

Assim, os dados indicam uma associação temporal positiva entre atividade policial e crimes patrimoniais do mês seguinte, mas não permitem concluir que o aumento da atividade policial provoque aumento ou redução da criminalidade.

### Pergunta 4 — Como o índice de recuperação de veículos evoluiu ao longo dos anos e entre as regiões?

<img width="832" height="395" alt="image" src="https://github.com/user-attachments/assets/127bf4c9-aeda-420b-8c26-39e9127b2cbc" />

O índice de recuperação de veículos apresentou variações importantes entre as regiões ao longo do período analisado. Entre 2019 e 2022, houve queda em boa parte das regiões, seguida por recuperação nos anos posteriores. O Interior apresentou a recuperação mais forte, chegando a aproximadamente 0,73 em 2026. Capital, Baixada Fluminense e Grande Niterói também apresentaram melhora após 2022, mas de forma mais gradual. Também foram identificadas **899 linhas com índice acima de 1, correspondendo a 9,42% dos registros com índice calculado**. Esses casos não foram considerados automaticamente como erro, pois veículos recuperados em um mês podem ter sido roubados ou furtados em períodos anteriores.

Os dados de 2026 estão incompletos, portanto devem ser interpretados com cautela.

### Pergunta 5 — Como evoluíram mensalmente os registros de feminicídio e tentativa de feminicídio a partir de outubro de 2024?

<img width="810" height="372" alt="image" src="https://github.com/user-attachments/assets/0474e6ba-1a5f-42f6-aaa6-840aeaec83ad" />

A partir de outubro de 2024, foram registrados **189 feminicídios** e **601 tentativas de feminicídio**. O maior número mensal de feminicídios ocorreu em **dezembro de 2025**, com **17 registros**, enquanto o maior número de tentativas ocorreu em **março de 2026**, com **40 registros**. Os dados mostram oscilações mensais, sem uma tendência contínua de crescimento ou queda. As tentativas de feminicídio permaneceram, em geral, acima dos registros de feminicídio. A análise considera somente o período a partir de outubro de 2024, e os dados de 2026 são parciais.

<img width="526" height="464" alt="image" src="https://github.com/user-attachments/assets/c545171d-f3e7-4b90-b214-0b78a24049c5" />


A análise considera somente os registros a partir de outubro de 2024, evitando interpretar os valores anteriores, que não possuem a mesma cobertura do indicador, como ausência de ocorrências. Os dados de 2026 também devem ser considerados como parciais, pois o ano ainda não está completo no conjunto analisado.

### Pergunta 6 — Quais meses apresentam os maiores registros de roubo de rua e roubo a estabelecimento comercial em cada ano, e esses meses se repetem ao longo dos anos?

<img width="670" height="330" alt="image" src="https://github.com/user-attachments/assets/5b9a8380-cd19-4277-9352-ebac72e4d24e" />

<img width="676" height="317" alt="image" src="https://github.com/user-attachments/assets/ea398d87-7216-4023-9be1-845fbe4e8f45" />

<img width="236" height="361" alt="image" src="https://github.com/user-attachments/assets/d2a4dc22-cf96-46c6-a536-28cbd89a2028" />

<img width="327" height="622" alt="image" src="https://github.com/user-attachments/assets/52848f9a-25e6-44a6-adcb-30d26f2e9c2b" />


A análise mostrou que os meses de maior ocorrência variam de um ano para outro, mas alguns se repetem com maior frequência. No roubo de rua, janeiro e março foram os meses que mais apareceram como o maior registro do ano, ocorrendo em 4 anos cada. Outubro e maio apareceram em 2 anos, enquanto julho apareceu uma vez.

No roubo a estabelecimento comercial, janeiro, março e maio foram os meses mais recorrentes, aparecendo como o maior registro anual em 3 anos cada. Abril e setembro apareceram em 2 anos cada. Assim, não existe um único mês que concentre os maiores registros em todos os anos. Porém, a repetição de alguns meses, especialmente janeiro e março, indica um padrão sazonal parcial, que varia conforme o tipo de crime e o ano analisado.

Os dados de 2026 são parciais, portanto o mês de maior ocorrência desse ano ainda pode mudar quando a série estiver completa.



## Autoavaliação

De forma geral, considero que os objetivos do MVP foram atingidos. Foi possível construir um pipeline completo no Databricks, passando pelas camadas Bronze, Silver e Gold, organizar os dados em tabelas fato e dimensão e responder às seis perguntas de negócio propostas no início do trabalho.

As análises permitiram observar a evolução dos crimes violentos por região, identificar as CISPs da Capital com maior volume de crimes patrimoniais, verificar a associação entre atividade policial e crimes do mês seguinte, analisar o índice de recuperação de veículos, acompanhar os registros de feminicídio e tentativa de feminicídio e verificar a repetição de meses com maiores registros de roubo de rua e roubo a comércio.

A maior dificuldade durante o desenvolvimento foi trabalhar com o PySpark e entender como organizar corretamente as transformações entre as camadas. No início, montar as tabelas Silver e Gold exigiu bastante atenção, principalmente para definir as chaves, fazer os tratamentos sem perder informações e entender como os dados deveriam chegar até a etapa de análise. Outra dificuldade importante foi a qualidade da base original. Algumas tabelas estavam bastante desorganizadas e apresentavam problemas como duplicidades, valores nulos, diferenças de tipo, campos com vírgula como separador decimal e problemas de encoding. Na base por CISP, por exemplo, foi necessário entender o campo `fase` para identificar por que existiam registros repetidos e manter a versão mais atualizada de cada registro. Também foi necessário corrigir o nome de Grande Niterói, que aparecia com problemas de codificação. :chatgpt-content-reference{index="1"}

Esses problemas fizeram com que a etapa Silver fosse uma das partes mais trabalhosas do projeto, pois foi necessário limpar, padronizar e validar os dados antes de utilizá-los. Na Gold, a principal dificuldade foi transformar essas tabelas já tratadas em uma estrutura que realmente ajudasse a responder às perguntas, criando as dimensões, as tabelas fato e as métricas derivadas utilizadas nas análises. Apesar das dificuldades, o trabalho ajudou a entender melhor a função de cada camada da arquitetura Medalhão e a importância de não realizar apenas transformações técnicas, mas também validar se os dados fazem sentido antes de utilizá-los em uma análise.

Trabalhos futuros:

Incorporar dados de população por município (IBGE) para normalizar comparações regionais na pergunta 1 sem o viés de tamanho populacional.
Buscar uma tabela de correspondência CISP → nome da delegacia para tornar a pergunta 2 mais legível.
Investigar a causalidade da pergunta 3 com métodos mais robustos (ex.: modelos de painel com efeitos fixos por CISP, para controlar o fato de que regiões mais violentas naturalmente têm mais atividade policial).
Investigar a origem da divergência de 0,55% entre letalidade_violenta e a soma dos seus 4 componentes documentados, concentrada em outubro/novembro de 2024 — possivelmente ligada ao processo de errata do ISP-RJ daquele período.
Automatizar a atualização mensal do pipeline conforme o ISP-RJ publica novos boletins.
