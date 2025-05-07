## README: Flattening de JSON com PySpark no Databricks

Este notebook demonstra um fluxo completo para importar um arquivo JSON armazenado em DBFS, extrair definições de colunas via metadados, e transformar um array de strings em uma tabela plana com nomes e tipos corretos de colunas, usando PySpark.

### Visão Geral

1. **Importação de Bibliotecas**: importa `requests`, `json`, módulos do PySpark (`functions`, `types`), e utilitários de sistema.
2. **Leitura do JSON**: carrega o arquivo `data.json` de `dbfs:/FileStore/` com a opção `multiLine=True` para suportar JSONs multilinha.
3. **Inspeção de Schema**: exibe a estrutura bruta (`printSchema`) e explora `meta.view.columns` para obter nomes (`fieldName`) e tipos (`dataTypeName`) das colunas definidas.

### Etapas Principais

1. **Explode de Metadados**

   * Usa `explode(meta.view.columns)` para extrair um DataFrame de definição de colunas.
   * Coleta essa informação em uma lista de tuplas `(nome, tipo)` para uso programático.

2. **Explode de Dados**

   * Aplica `explode(data)` ao array de arrays de strings, gerando linhas unitárias com `values: array<string>`.

3. **Mapeamento Dinâmico de Colunas**

   * Gera uma lista de expressões PySpark (`exprs`), usando `getItem(i)` para cada posição do array e aplicando `cast()` conforme o tipo extraído.
   * Cada expressão recebe um alias com o `fieldName` correto.

4. **Flatten e Casting**

   * Aplica `select(*exprs)` no DataFrame explosado, resultando em um DataFrame plano com todas as colunas nomeadas e tipadas.

5. **Criação de View Temporária**

   * Registra o DataFrame final em uma Temp View chamada `final_table` para possibilitar consultas SQL diretas.

6. **Consulta SQL**

   * Exemplo de consulta: `SELECT * FROM final_table;` via `%sql`.

### Testes Adicionais

* O notebook inclui células de teste lendo um arquivo `sample.json`, executando explodes em estruturas distintas (`goods.customers`, `goods.orders`) e validando o funcionamento das funções de flattening.

---

> **Resultado**: Ao final, obtém-se uma tabela única (`final_table`) pronta para análises e gravação como Delta, sem necessidade de modelagem manual de várias tabelas intermediárias.
