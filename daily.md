# Daily — Databricks medallion (AdventureWorks)

Diário de estudo e projeto: DABs + Terraform + CI/CD no GitHub + Databricks.
Uma seção por dia, e dentro dela uma subseção por PR (PRs parecidos viram uma só).

## Índice

- [2026-10-14 e 2026-10-16: bundle inicial](#2026-10-14-e-2026-10-16--bundle-inicial)
- [2026-11-01: primeiro pipeline, CI/CD e deploy](#2026-11-01--primeiro-pipeline-cicd-e-deploy)
- [2026-11-06: Terraform, seeder e CI](#2026-11-06--terraform-seeder-e-ci)
- [2026-11-07: bind, dedup, options, regras de domínio e permissões](#2026-11-07--bind-dedup-options-regras-de-domínio-e-permissões)

## Como consolidar no fim do dia

Copie o modelo, preencha e cole no fim do arquivo. Datas em horário local (UTC-3).

```markdown
## AAAA-MM-DD — tema do dia

### PR #N (+ #M, se forem parecidos) — título curto
- **O que mudou:**
- **Motivo:**
- **Erros e correções:**
  - erro → causa → correção
- **O que aprendi:**
- **O que melhorar:**
```

---

## 2026-10-14 e 2026-10-16 — bundle inicial

Poucos detalhes: o histórico desta conversa começa depois. Fica só o que o Git registra.

### PR #1 e #2 — projeto inicial e primeira configuração
- **O que mudou:** bundle `dev_platform` criado com o template `default-python` (14/10). Depois, ajustes de configuração do Databricks e renomeação de variáveis no `databricks.yml` (16/10).
- **Motivo:** ter um ponto de partida de DABs para treinar deploy.
- **O que melhorar:** anotar o motivo de cada decisão no dia em que ela acontece. Aqui já não dá para reconstruir.

---

## 2026-11-01 — primeiro pipeline, CI/CD e deploy

### PR #3 + #4 + #5 — pipeline medalhão do customer, CI/CD e deploy serverless
- **O que mudou:**
  - Padrão de projeto: um `Pipeline` único com Reader, Transformers e Writer injetados (Strategy). Uma factory por transição de camada: `landing → raw` e `raw → trusted`.
  - Transformers genéricos em `transformers/` e regras de domínio em `domains/customer/`.
  - Entrypoints finos, jobs por camada, um orquestrador com `run_job_task` e `depends_on`.
  - CI (validate + testes) e CD (deploy em `-t prod` no push para `main`), com o service principal `sp_databricks_bundle` recriado e as secrets no GitHub.
  - 3 testes baratos: transformação com DataFrame pequeno, contrato de schema e composição.
  - Trabalho dessa data que só entrou no `main` pelo PR #32: `.gitignore` reescrito, taxonomia de tags (`conf/data_tags.yml` + função que aplica as tags) e README.
- **Motivo:** o primeiro deploy integrado de ponta a ponta da tabela customer em prod, com regras de negócio separadas das genéricas e uma ordem clara entre camadas.
- **Erros e correções:**
  - `F.col(col_name.lower())` → baixava o nome da coluna, não o valor → `F.lower(F.col(col_name))`.
  - Imports `from core.common...` e duas classes com o mesmo nome → `dev_platform.core...` e `NormalizeMaritalStatus`.
  - `include: jobs/**/*.yml` carregava só um nível → o `run_job_task` apontava para um job que não existia, e o `validate` passou mesmo assim → globs explícitos (`jobs/*.yml`, `jobs/*/*.yml`, `jobs/*/*/*.yml`).
  - Secrets criadas como *environment secrets* → o job do CI não as via → duas *repository secrets* (`DATABRICKS_CLIENT_ID` e `DATABRICKS_CLIENT_SECRET`).
  - No serverless, `delta>=0.4.2` em `dependencies` (pacote errado do PyPI) → removido, o runtime já traz o Delta.
  - Wheel com `requires-python ==3.12.*` num serverless com Python 3.11 → relaxado para `>=3.10` (#5).
  - SP deletado e recriado → App ID novo e grants velhos → atualizar `variables.yml` e refazer os grants.
  - O display name do SP não resolve em `permissions` → usar o Application ID (UUID).
- **O que aprendi:**
  - Camadas (landing, raw, trusted) têm duas ordens: a lista de transformers (dentro da camada) e o `depends_on` do job (entre camadas).
  - `bundle validate` não pega referência de recurso quebrada nem caminho de wheel. Só o deploy ou o `plan` pega.
  - Secret de ambiente do GitHub só aparece em job que declara `environment:`.
- **O que melhorar:**
  - Rodar `bundle plan` no CI, além do `validate`.
  - Teste de composição precisa de SparkSession real: a factory chama `col(...)` ao construir.

---

## 2026-11-06 — Terraform, seeder e CI

### PR #32 + #33 — Terraform para a infra, seeder de landing e CI
- **O que mudou:**
  - **Terraform** (`infra/`): schemas `raw`, `trusted` e `enriched`, e os grants do SP. DABs ficou só com jobs e wheel, então `conf/schemas.yml` foi removido.
  - **Seeder** `seed-customer`: escreve lotes de 8 linhas na landing, em modo serverless, com job agendado a cada 30 minutos.
  - **CI** consertado, com `databricks-connect` fora do grupo dev e `requires-python = ">=3.11,<3.13"`.
  - Co-author removido dos commits.
  - **#33:** o job de seed passa a nascer `PAUSED`.
- **Motivo:** treinar o ciclo completo (Terraform para infra, DABs para jobs, CD automático) e ter dado novo chegando sem upload manual, sem estourar o custo.
- **Erros e correções:**
  - **Quase destruí o prod.** O plan mostrou `catalog_name advworks_prd → advworks_dev` e 5 destroys. A causa era `var.catalog` com default `advworks_dev`, enquanto o state do prod guardava objetos de `advworks_prd`. Parei antes do apply. A correção foi `local.catalog = terraform.workspace == "prod" ? ...` nos **dois** arquivos, `schemas.tf` e `grants.tf` (o segundo ficou esquecido e o plan ainda mostrou 3 destroys).
  - `Duplicate local value definition` → `catalog` estava definido em dois arquivos.
  - Grant criado antes do schema → o schema era uma string, sem aresta de dependência → referenciar o recurso (`databricks_schema.layers[each.key]`).
  - Seed gerado com `pa.timestamp("ns")` → o Spark não lê → `"us"`.
  - CI falhou: o `databricks-connect` (que exige Python 3.12 ou 3.13) tinha voltado ao grupo dev e o runner usou 3.14 → removido do dev, com versão do Python limitada.
  - Seed entrou em prod `UNPAUSED` e rodaria sozinho a cada 30 minutos → `PAUSED` (#33).
  - `git commit --amend` mudou o hash → push exigiu `--force-with-lease`.
- **O que aprendi:**
  - `terraform import` adota o que já existe, não é "criar se não existir".
  - `databricks_grants` é autoritativo (apaga o que não está na lista). `databricks_grant` é aditivo, e é o que serve para recursos compartilhados.
  - O state do Terraform é isolado por workspace. Ler o plan **antes** do apply é o que evita o desastre.
  - `terraform.workspace` + `locals` troca dev e prod sem duplicar código.
- **O que melhorar:**
  - `terraform plan` no CI, para ver destroy no PR e não no terminal.
  - Todo job novo nasce `PAUSED` por padrão.
  - Antes de commitar `pyproject.toml`, rodar `uv lock` e o `pytest` da forma que o CI roda.

---

## 2026-11-07 — bind, dedup, options, regras de domínio e permissões

### PR #34 + #35 — renomear jobs com `bind`, orquestrador seed → raw → trusted e dedup
- **O que mudou:**
  - Chaves dos jobs renomeadas (`raw_customer_ingestion`, `trusted_customer_ingestion`, `customer_seed`, `customer_orchestrator`) sem recriar os jobs em prod.
  - Orquestrador com a sequência seed → raw → trusted.
  - Novo `DeduplicateTransformer`: 1 linha por `customerid`, ficando a ingestão mais recente, antes do MERGE.
  - README inicial.
- **Motivo:** testar `bind`/`unbind` em prod e corrigir o MERGE da trusted, que falhava.
- **Erros e correções:**
  - `unbind` → renomear no yml → `bind <chave nova> <job_id>` → `plan`. O `unbind` exige a chave **antiga** ainda no config.
  - Orquestrador com task dependendo dela mesma e `depends_on` para uma task que não existia → bloco de tasks corrigido.
  - `bundle deploy --dry-run` não existe → `bundle plan`.
  - `plan` mostrou 4 add + 4 change + 0 delete → os 4 add eram só as permissions dos jobs (o `unbind` as tira do state).
  - `DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE` → a raw guarda uma linha por endereço, o MERGE exige uma por chave → dedup.
  - `git push` sem credencial → `gh auth setup-git`.
- **O que aprendi:**
  - Renomear a chave de um recurso **sem** `bind` é destroy + create.
  - O state é compartilhado entre o terminal local e o CD. Depois do deploy local, o rename precisa chegar ao `main` logo, senão o próximo CD destrói os jobs.
  - Falha no MERGE faz rollback atômico e a tabela fica intacta.
- **O que melhorar:** o dedup assume `inserted_date` como desempate. Se a coluna sumir, a ordem fica arbitrária. Vale um teste de contrato.

### PR #36 + #37 + #38 — options, liquid clustering e regras de domínio (3 PRs empilhados)
- **O que mudou:**
  - **#36:** `options` em readers e writers. `partition_by` ou `cluster_by`, mutuamente exclusivos. `schema_evolution` no `TableMergeWriter`.
  - **#37:** regras do customer movidas para `domains/customer/rules/`, `EmailFlagTransformer`, decorators `Logging*` e seed job movido para `jobs/workflows/seed/`.
  - **#38:** renda como `decimal(18,2)` (`minincome` e `maxincome`), classe de renda (`low`, `middle-low`, `middle`, `upper-middle`, `high`, `Not Available`), idade a partir de `BirthDate` e normalização de gênero e estado civil. Clustering por `customerid` e `countryregioncode`.
- **Motivo:** um único PR misturava infraestrutura do writer, refatoração e regra nova. Separei em PRs que dá para revisar, com ordem de merge.
- **Erros e correções:**
  - `overwriteSchema=true` aplicado **depois** das options do chamador anulava as options → o chamador tem que vencer o default (`opts.update(self._options)`).
  - Imports dos testes quebrados e Pylance sem enxergar `src/` → `extraPaths` no `.vscode` da raiz e `pythonpath` no `pyproject`.
  - Teste de composição comparava o tipo exato → decorators `Logging*` mudam o tipo → helper `_unwrap`.
  - Seed job movido de pasta: o caminho do wheel `../../dist/*.whl` quebrou. O `validate` passou e o `bundle plan` falhou → `../../../dist/*.whl`.
  - CI do PR #37 vermelho depois do merge do #36 → o GitHub testava uma combinação antiga e *re-run* reaproveita a mesma → fechar e reabrir, ou "Update branch".
  - O cast de renda do `customerincometransformer.py` não surtia efeito: `trim` e `coalesce(..., lit("0"))` devolviam string de novo. Com ANSI ligado, `greater than 100000` quebrava com `CAST_INVALID_INPUT` (e `getItem(1)` com `INVALID_ARRAY_INDEX`) → `regexp_extract_all`, `try_element_at` e `try_cast`.
  - Três transformers que **não faziam nada, sem avisar**: `columns="birthdate"` (string, minúscula; a coluna é `BirthDate`), ranking apontando para `YearlyIncome` em vez de `minincome`, e `months_between` sem o 2º argumento. Os testes antigos passavam. O contrato de schema passou a conferir as colunas novas.
  - Conflito "fantasma" no PR #38 (merges em cruz entre PR 1, 2 e 3) → merge do `main` na branch, sem nenhuma diferença de conteúdo.
- **O que aprendi:**
  - `ColumnValidator` descarta coluna em silêncio, e é sensível a maiúsculas e minúsculas. Erro de nome vira no-op, e o teste passa.
  - Delta MERGE ignora as options do `DataFrameWriter`.
  - Com ANSI ligado (padrão no Spark 4 e no serverless), cast inválido e índice fora do array **quebram**. `try_cast` e `try_element_at` devolvem nulo.
  - `liquid clustering` só vale na criação da tabela. Em tabela existente: `ALTER TABLE ... CLUSTER BY` e `OPTIMIZE`.
  - PRs empilhados: merge commit funciona, squash gera ruído.
- **O que melhorar:**
  - `ColumnValidator` deveria avisar ou falhar quando a coluna pedida não existe.
  - Os testes de composição deveriam checar o **efeito** (a coluna nova apareceu), não só que o pipeline monta.
  - `rules/*` e `email.py` ainda sem teste. Ruff acusa `B008` (`ColumnValidator()` como default de argumento).
  - Conferir `DESCRIBE advworks_prd.trusted.customer` antes do 1º run: se `minincome` já existir como string, o MERGE falha na troca de tipo.

### PR #39 — schema evolution no MERGE para serverless
- **O que mudou:** o MERGE usa `.withSchemaEvolution()` no builder do Delta. Saiu o código que ligava e restaurava a conf da sessão.
- **Motivo:** o orquestrador em prod falhou com `CONFIG_NOT_AVAILABLE.SERVERLESS_DELTA_SCHEMA_AUTO_MERGE_ENABLED`.
- **Erros e correções:**
  - `spark.databricks.delta.schema.autoMerge.enabled` não é aceito no serverless → evolução por comando, no builder.
  - A mensagem sugere `.option("mergeSchema", "true")`, mas isso é do `DataFrameWriter`, e o MERGE do `DeltaTable` ignora → `withSchemaEvolution()`.
- **O que aprendi:** o que funciona em cluster pode não funcionar no serverless. Conf de sessão é o primeiro suspeito. O teste local usa um Delta simulado, então só o run em prod prova.
- **O que melhorar:** um teste de integração com Delta real (cluster ou serverless), fora do CI padrão.

### PR #40 + #41 — higiene do repositório e permissão do SP (2 mudanças pequenas)
- **O que mudou:**
  - **#40:** notebook de `connect/` sai do Git (continua no disco) e `/typings/` entra no `.gitignore`.
  - **#41:** `SELECT` entra nos grants do SP nos schemas `raw`, `trusted` e `enriched`.
- **Motivo:** o `.gitignore` já ignorava `connect/*.ipynb`, mas o notebook estava versionado. E o job de trusted falhava com `PERMISSION_DENIED: does not have SELECT on Table 'advworks_prd.raw.customer'`.
- **Erros e correções:**
  - Ignorar não vale para arquivo já rastreado → `git rm --cached`.
  - `MODIFY` não dá leitura no Unity Catalog → o job lê a raw e o MERGE lê o destino → `SELECT` junto.
  - `grants get` voltou vazio: ele só mostra grants **diretos** da tabela → `grants get-effective` mostra os herdados do schema.
  - `git pull` com branches divergentes → `git pull --no-rebase` (e `git config pull.rebase false`). Antes, backup do notebook e descarte das cópias locais que já estavam no `main`.
- **O que aprendi:**
  - Permissão de escrita (`MODIFY`) não implica leitura (`SELECT`). Testes locais com o seu usuário (dono da tabela) escondem esse erro.
  - O `grants.tf` precisa refletir o que foi aplicado, senão o próximo `apply` remove a permissão.
- **O que melhorar:** rodar um job pelo SP em prod logo após mudar grants. E aplicar o Terraform também no workspace de dev (`default`), que ainda não recebeu o `SELECT`.

### PR #43 — README
- **O que mudou:** itens concluídos do roadmap (deploys 1 e 2) marcados no README.

### Fora de PR neste dia
- Working tree limpo: o que sobrou (mover `apply_tags` para `utils/`, `splitcolumn.py` e a remoção de `fixtures/.gitkeep`) foi guardado em `git stash`.
- **Pendente:** o entry point `apply-tags` do `pyproject.toml:36` aponta para `entrypoints/apply_tags.py`. Se o arquivo voltar a sair daquela pasta, o entry point quebra.
- **Pendente:** rodar o orquestrador em prod depois do deploy do #39, para provar o fix do MERGE.
