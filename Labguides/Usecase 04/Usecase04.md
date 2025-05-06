# Caso de uso 04: Análise moderna em escala de nuvem com o Azure Databricks e o Microsoft Fabric

**Introdução**

Neste laboratório, você explorará a integração do Azure Databricks com o
Microsoft Fabric para criar e gerenciar um lakehouse usando a
Arquitetura Medallion, criar uma tabela Delta com a ajuda da sua conta
do Azure Data Lake Storage (ADLS) Gen2 usando o Azure Databricks e
inserir dados com o Azure Databricks. Este guia prático o conduzirá
pelas etapas necessárias para criar um lakehouse carregar dados e
explorar as camadas de dados estruturados para facilitar a análise e a
geração de relatórios de dados eficientes.

A Arquitetura Medallion consiste em três camadas (ou zonas) distintas.

- Bronze: Também conhecida como zona bruta, essa primeira camada
  armazena dados de origem em seu formato original. Normalmente, os
  dados nessa camada são apenas anexados e imutáveis.

- Prata: Também conhecida como zona enriquecida, esta camada armazena
  dados provenientes da camada bronze. Os dados brutos foram limpos e
  padronizados e agora estão estruturados como tabelas (linhas e
  colunas). Também podem ser integrados a outros dados para fornecer uma
  visão corporativa de todas as entidades de negócios, como clientes,
  produtos e outros.

- Gold: Também conhecida como zona de curadoria, esta camada final
  armazena dados provenientes da camada Silver. Os dados são refinados
  para atender a requisitos específicos de negócios e análises
  posteriores. As tabelas geralmente seguem o design de esquema em
  estrela, que suporta o desenvolvimento de modelos de dados otimizados
  para desempenho e usabilidade.

**Objetivos** :

- Entenda os princípios da Arquitetura Medallion no Microsoft Fabric
  Lakehouse.

- Implemente um processo estruturado de gerenciamento de dados usando
  camadas Medallion (Bronze, Silver, Gold).

- Transforme dados brutos em dados validados e enriquecidos para
  análises e relatórios avançados.

- Aprenda as melhores práticas para segurança de dados, CI/CD e consulta
  de dados eficiente.

- Carregue dados para o OneLake com o explorador de arquivos OneLake .

- Use um notebook Fabric para ler dados no OneLake e escrevê-los como
  uma tabela Delta.

- Analise e transforme dados com o Spark usando um notebook Fabric.

- Consulte uma cópia de dados no OneLake com SQL.

- Crie uma tabela Delta na sua conta do Azure Data Lake Storage (ADLS)
  Gen2 usando o Azure Databricks.

- Crie um atalho do OneLake para uma tabela Delta no ADLS.

- Use o Power BI para analisar dados por meio do atalho do ADLS.

- Leia e modifique uma tabela Delta no OneLake com o Azure Databricks.

# Exercício 1: Trazendo seus dados de amostra para o Lakehouse

Neste exercício, você passará pelo processo de criação de um lakehouse e
carregamento de dados usando o Microsoft Fabric.

Tarefa: trilha

## **Tarefa 1: Crie um workspace do Fabric**

Nesta tarefa, você criará um workspace do Fabric. O workspace contém
todos os itens necessários para este tutorial do Lakehouse incluindo o
Lakehouse fluxos de dados, pipelines do Data Factory, notebooks,
conjuntos de dados do Power BI e relatórios.

1.  Abra seu navegador, navegue até a barra de endereço e digite ou cole
    o seguinte URL:
    [https://app.fabric.microsoft.com/](https://app.fabric.microsoft.com/,)
    então pressione o botão **Enter.**

> ![A search engine window with a red box Description automatically
> generated with medium confidence](./media/image1.png)

2.  Retorne à janela do **Power BI**. No menu de navegação esquerdo da
    página inicial do Power BI, navegue e clique em **Workspaces**.

![](./media/image2.png)

3.  No painel Workspaces, clique no botão **+** **New workspace.**

> ![](./media/image3.png)

4.  No painel **Create a workspace** que aparece no lado direito, insira
    os seguintes detalhes e clique no botão **Apply**.

[TABLE]

> ![](./media/image4.png)

![A screenshot of a computer Description automatically
generated](./media/image5.png)

5.  Aguarde a conclusão da implementação. Ela leva de 2 a 3 minutos.

![](./media/image6.png)

## **Tarefa 2: Criar um lakehouse**

1.  Na página **Power BI Fabric Lakehouse Tutorial-XX**, clique no ícone
    do **Power BI** localizado no canto inferior esquerdo e selecione
    **Data Engineering**.

> ![](./media/image7.png)

2.  Na página **Synapse** **Data Engineering** **Home**, selecione
    **Lakehouse** para criar uma Lakehouse.

![](./media/image8.png)

![A screenshot of a computer Description automatically
generated](./media/image9.png)

3.  Na caixa de diálogo **New lakehouse**, digite **wwilakehouse** no
    campo **Name**, clique no botão **Create** e abra o novo lakehouse.

> **Nota** : Certifique-se de remover o espaço antes de
> **wwilakehouse**.
>
> ![](./media/image10.png)
>
> ![A screenshot of a computer Description automatically
> generated](./media/image11.png)
>
> ![](./media/image12.png)

4.  Você verá uma notificação informando **Successfully created SQL
    endpoint**.

> ![](./media/image13.png)

# Exercício 2: Implementando a Arquitetura Medallion usando o Azure Databricks

## **Tarefa 1: Configurando a camada de bronze**

1.  Na página **wwilakehouse**, selecione o ícone Mais ao lado dos
    arquivos (…) e selecione **New subfolder**

![](./media/image14.png)

2.  No pop-up, informe o nome da pasta como **bronze** e selecione
    Create.

![A screenshot of a computer Description automatically
generated](./media/image15.png)

3.  Agora, selecione o ícone ao lado dos arquivos bronze (…), selecione
    **Upload** e, em seguida, **upload files**.

![A screenshot of a computer Description automatically
generated](./media/image16.png)

4.  No painel **upload file**, selecione o botão **upload file**. Clique
    no **botão Browse** e navegue até **C:\LabFiles.** Em seguida,
    selecione os arquivos de dados de vendas requerido (2019,
    2020, 2021) e clique no botão **Open**.

Em seguida, selecione **Upload** para enviar os arquivos para a nova
pasta 'bronze' no seu Lakehouse.

![A screenshot of a computer Description automatically
generated](./media/image17.png)

> ![A screenshot of a computer Description automatically
> generated](./media/image18.png)

5.  Clique na pasta **bronze** para validar se os arquivos foram
    enviados com sucesso e se estão sendo refletidos.

![A screenshot of a computer Description automatically
generated](./media/image19.png)

# Exercício 3: Transformando dados com Apache Spark e consulta com SQL na arquitetura Medallion

## **Tarefa 1: Transformar dados e carregar na tabela Delta Silver**

Na página **wwilakehouse**, navegue e clique em **Open notebook** na
barra de comando e selecione **New notebook**.

![A screenshot of a computer Description automatically
generated](./media/image20.png)

1.  Selecione a primeira célula (que atualmente é uma célula *de
    código*) e, na barra de ferramentas dinâmica no canto superior
    direito, use o botão **M↓ para converter a célula em uma célula de
    markdown** .

![A screenshot of a computer Description automatically
generated](./media/image21.png)

2.  Quando a célula muda para uma célula markdown, o texto que ela
    contém é renderizado.

![A screenshot of a computer Description automatically
generated](./media/image22.png)

3.  Use o botão **🖉** (Edit) para alternar a célula para o modo de
    edição, substitua todo o texto e modifique a marcação da seguinte
    maneira:

CodeCopy

\# Sales order data exploration

Use the code in this notebook to explore sales order data.

![A screenshot of a computer Description automatically
generated](./media/image23.png)

![A screenshot of a computer Description automatically
generated](./media/image24.png)

4.  Clique em qualquer lugar do notebook fora da célula para parar de
    editá-lo e ver a marcação renderizada.

![A screenshot of a computer Description automatically
generated](./media/image25.png)

5.  Use o ícone + Code abaixo da saída da célula para adicionar uma nova
    célula de código ao notebook.

![A screenshot of a computer Description automatically
generated](./media/image26.png)

6.  Agora, use o notebook para carregar os dados da camada bronze em um
    Spark DataFrame .

Selecione a célula existente no notebook que contém um código simples
comentado. Destaque e exclua essas duas linhas — você não precisará
desse código.

*Observação: os notebooks permitem executar código em diversas
linguagens, incluindo Python, Scala e SQL. Neste exercício, você usará
PySpark e SQL. Você também pode adicionar células Markdown para fornecer
texto formatado e imagens para documentar seu código.*

Para isso, insira o seguinte código e clique em **Executar** .

CodeCopy

from pyspark.sql.types import \*

\# Create the schema for the table

orderSchema = StructType(\[

StructField("SalesOrderNumber", StringType()),

StructField("SalesOrderLineNumber", IntegerType()),

StructField("OrderDate", DateType()),

StructField("CustomerName", StringType()),

StructField("Email", StringType()),

StructField("Item", StringType()),

StructField("Quantity", IntegerType()),

StructField("UnitPrice", FloatType()),

StructField("Tax", FloatType())

\])

\# Import all files from bronze folder of lakehouse

df = spark.read.format("csv").option("header",
"true").schema(orderSchema).load("Files/bronze/\*.csv")

\# Display the first 10 rows of the dataframe to preview your data

display(df.head(10))

![](./media/image27.png)

***Observação** : Como esta é a primeira vez que você executa qualquer
código Spark neste notebook, uma sessão Spark precisa ser iniciada. Isso
significa que a primeira execução pode levar cerca de um minuto para ser
concluída. As execuções subsequentes serão mais rápidas.*

![A screenshot of a computer Description automatically
generated](./media/image28.png)

7.  O código que você executou carregou os dados dos arquivos CSV na
    pasta **bronze** em um dataframe do Spark e, em seguida, exibiu as
    primeiras linhas do dataframe .

> **Observação** : você pode limpar, ocultar e redimensionar
> automaticamente o conteúdo da saída da célula selecionando o menu
> **…** no canto superior esquerdo do painel de saída.

8.  Agora você **adicionará colunas para validação e limpeza de dados**
    usando um PySpark dataframe para adicionar colunas e atualizar os
    valores de algumas das colunas existentes. Use o botão + para
    **adicionar um novo bloco de código** e adicione o seguinte código à
    célula:

> CodeCopy
>
> from pyspark.sql.functions import when, lit, col, current_timestamp,
> input_file_name
>
> \# Add columns IsFlagged, CreatedTS and ModifiedTS
>
> df = df.withColumn("FileName", input_file_name()) \\
>
> .withColumn("IsFlagged", when(col("OrderDate") \<
> '2019-08-01',True).otherwise(False)) \\
>
> .withColumn("CreatedTS", current_timestamp()).withColumn("ModifiedTS",
> current_timestamp())
>
> \# Update CustomerName to "Unknown" if CustomerName null or empty
>
> df = df.withColumn("CustomerName", when((col("CustomerName").isNull()
> |
> (col("CustomerName")=="")),lit("Unknown")).otherwise(col("CustomerName")))
>
> A primeira linha do código importa as funções necessárias do PySpark.
> Em seguida, você adiciona novas colunas ao *dataframe* para poder
> rastrear o nome do arquivo de origem, se o pedido foi marcado como
> anterior ao ano fiscal de interesse, e quando a linha foi criada e
> modificada.
>
> Por fim, você atualiza a coluna CustomerName para “Unknown” se ela for
> nula ou vazia.
>
> Em seguida, execute a célula para executar o código usando o botão
> **\*\* ▷** (*Run cell*)\* \*.

![A screenshot of a computer Description automatically
generated](./media/image29.png)

![A screenshot of a computer Description automatically
generated](./media/image30.png)

9.  Em seguida, você definirá o esquema para a tabela **sales_silver**
    no banco de dados sales usando o formato Delta Lake. Crie um novo
    bloco de código e adicione o seguinte código à célula:

> CodeCopy

from pyspark.sql.types import \*

from delta.tables import \*

\# Define the schema for the sales_silver table

silver_table_schema = StructType(\[

    StructField("SalesOrderNumber", StringType(), True),

    StructField("SalesOrderLineNumber", IntegerType(), True),

    StructField("OrderDate", DateType(), True),

    StructField("CustomerName", StringType(), True),

    StructField("Email", StringType(), True),

    StructField("Item", StringType(), True),

    StructField("Quantity", IntegerType(), True),

    StructField("UnitPrice", FloatType(), True),

    StructField("Tax", FloatType(), True),

    StructField("FileName", StringType(), True),

    StructField("IsFlagged", BooleanType(), True),

    StructField("CreatedTS", TimestampType(), True),

    StructField("ModifiedTS", TimestampType(), True)

\])

\# Create or replace the sales_silver table with the defined schema

DeltaTable.createIfNotExists(spark) \\

    .tableName("wwilakehouse.sales_silver") \\

    .addColumns(silver_table_schema) \\

    .execute()

   

10. Execute a célula para iniciar o código usando o botão **\*\* ▷**
    (*Run cell*)\* \*.

11. Selecione **…** na seção Tabelas do painel do explorador do
    Lakehouse e selecione **Refresh**. Agora você deverá ver a nova
    tabela **sales_silver** listada. O **▲** (triangle icon) indica que
    é uma tabela Delta.

> **Observação** : se você não vir a nova tabela, aguarde alguns
> segundos e selecione **Refresh** novamente ou atualize toda a guia do
> navegador.
>
> ![A screenshot of a computer Description automatically
> generated](./media/image31.png)
>
> ![A screenshot of a computer Description automatically
> generated](./media/image32.png)

12. Agora você executará uma **upsert operation** em uma tabela Delta,
    atualizando registros existentes com base em condições específicas e
    inserindo novos registros quando nenhuma correspondência for
    encontrada. Adicione um novo bloco de código e cole o seguinte
    código:

> CodeCopy
>
> from pyspark.sql.types import \*
>
> from pyspark.sql.functions import when, lit, col, current_timestamp,
> input_file_name
>
> from delta.tables import \*
>
> \# Define the schema for the source data
>
> orderSchema = StructType(\[
>
> StructField("SalesOrderNumber", StringType(), True),
>
> StructField("SalesOrderLineNumber", IntegerType(), True),
>
> StructField("OrderDate", DateType(), True),
>
> StructField("CustomerName", StringType(), True),
>
> StructField("Email", StringType(), True),
>
> StructField("Item", StringType(), True),
>
> StructField("Quantity", IntegerType(), True),
>
> StructField("UnitPrice", FloatType(), True),
>
> StructField("Tax", FloatType(), True)
>
> \])
>
> \# Read data from the bronze folder into a DataFrame
>
> df = spark.read.format("csv").option("header",
> "true").schema(orderSchema).load("Files/bronze/\*.csv")
>
> \# Add additional columns
>
> df = df.withColumn("FileName", input_file_name()) \\
>
> .withColumn("IsFlagged", when(col("OrderDate") \< '2019-08-01',
> True).otherwise(False)) \\
>
> .withColumn("CreatedTS", current_timestamp()) \\
>
> .withColumn("ModifiedTS", current_timestamp()) \\
>
> .withColumn("CustomerName", when((col("CustomerName").isNull()) |
> (col("CustomerName") == ""),
> lit("Unknown")).otherwise(col("CustomerName")))
>
> \# Define the path to the Delta table
>
> deltaTablePath = "Tables/sales_silver"
>
> \# Create a DeltaTable object for the existing Delta table
>
> deltaTable = DeltaTable.forPath(spark, deltaTablePath)
>
> \# Perform the merge (upsert) operation
>
> deltaTable.alias('silver') \\
>
> .merge(
>
> df.alias('updates'),
>
> 'silver.SalesOrderNumber = updates.SalesOrderNumber AND \\
>
> silver.OrderDate = updates.OrderDate AND \\
>
> silver.CustomerName = updates.CustomerName AND \\
>
> silver.Item = updates.Item'
>
> ) \\
>
> .whenMatchedUpdate(set = {
>
> "SalesOrderLineNumber": "updates.SalesOrderLineNumber",
>
> "Email": "updates.Email",
>
> "Quantity": "updates.Quantity",
>
> "UnitPrice": "updates.UnitPrice",
>
> "Tax": "updates.Tax",
>
> "FileName": "updates.FileName",
>
> "IsFlagged": "updates.IsFlagged",
>
> "ModifiedTS": "current_timestamp()"
>
> }) \\
>
> .whenNotMatchedInsert(values = {
>
> "SalesOrderNumber": "updates.SalesOrderNumber",
>
> "SalesOrderLineNumber": "updates.SalesOrderLineNumber",
>
> "OrderDate": "updates.OrderDate",
>
> "CustomerName": "updates.CustomerName",
>
> "Email": "updates.Email",
>
> "Item": "updates.Item",
>
> "Quantity": "updates.Quantity",
>
> "UnitPrice": "updates.UnitPrice",
>
> "Tax": "updates.Tax",
>
> "FileName": "updates.FileName",
>
> "IsFlagged": "updates.IsFlagged",
>
> "CreatedTS": "current_timestamp()",
>
> "ModifiedTS": "current_timestamp()"
>
> }) \\
>
> .execute()

13. Execute a célula para iniciar o código usando o botão **\*\* ▷**
    (Run cell)\* \*.

![A screenshot of a computer Description automatically
generated](./media/image33.png)

Esta operação é importante porque permite atualizar registros existentes
na tabela com base nos valores de colunas específicas e inserir novos
registros quando nenhuma correspondência for encontrada. Este é um
requisito comum ao carregar dados de um sistema de origem que pode
conter atualizações de registros existentes e novos.

Agora você tem dados em sua tabela delta Silver prontos para mais
transformação e modelagem.

Você extraiu com sucesso os dados da sua camada bronze, transformou-os e
carregou-os em uma tabela Delta Silver. Agora, você usará um novo
notebook para transformar os dados ainda mais, modelá-los em um esquema
estrela e carregá-los em tabelas Delta Gold.

*Observe que você poderia ter feito tudo isso em um único notebook, mas,
para os propósitos deste exercício, você está usando notebooks separados
para demonstrar o processo de transformação de dados de bronze para
Silver e, em seguida, de Silver para Gold. Isso pode ajudar na
depuração, solução de problemas e reutilização* .

## **Tarefa 2: Carregar dados nas tabelas Delta Gold**

1.  Retorne à página inicial do Fabric Lakehouse Tutorial-29.

> ![A screenshot of a computer Description automatically
> generated](./media/image34.png)

2.  Selecione **wwilakehouse .**

![A screenshot of a computer Description automatically
generated](./media/image35.png)

3.  No painel do explorador do lakehouse, você deve ver a tabela
    **sales_silver** listada na seção **Tables** do painel do
    explorador.

![A screenshot of a computer Description automatically
generated](./media/image36.png)

4.  Agora, crie um novo notebook chamado **Transform data for Gold**.
    Para isso, navegue e clique em **Open notebook** na barra de
    comandos e selecione **New notebook**.

![A screenshot of a computer Description automatically
generated](./media/image37.png)

5.  No bloco de código existente, remova o texto padrão e **adicione o
    seguinte código** para carregar dados no seu dataframe e começar a
    construir seu esquema em estrela. Depois, execute-o:

> CodeCopy

\# Load data to the dataframe as a starting point to create the gold
layer

df = spark.read.table("wwilakehouse.sales_silver")

\# Display the first few rows of the dataframe to verify the data

df.show()

![A screenshot of a computer Description automatically
generated](./media/image38.png)

6.  Em seguida**, adicione um novo bloco de código**, cole o código
    abaixo para criar sua tabela de dimensão de data e execute-o:

 from pyspark.sql.types import \*

 from delta.tables import\*

   

 # Define the schema for the dimdate_gold table

 DeltaTable.createIfNotExists(spark) \\

     .tableName("wwilakehouse.dimdate_gold") \\

     .addColumn("OrderDate", DateType()) \\

     .addColumn("Day", IntegerType()) \\

     .addColumn("Month", IntegerType()) \\

     .addColumn("Year", IntegerType()) \\

     .addColumn("mmmyyyy", StringType()) \\

     .addColumn("yyyymm", StringType()) \\

     .execute()

![A screenshot of a computer Description automatically
generated](./media/image39.png)

**Nota** : Você pode executar o comando display(df) a qualquer momento
para verificar o andamento do seu trabalho. Neste caso, você executaria
'display(dfdimDate_gold)' para ver o conteúdo dimDate_gold. dataframe.

7.  Em um novo bloco de código, **adicione e execute o seguinte código**
    para criar um dataframe para sua dimensão de data, **dimdate_gold**
    :

> CodeCopy

from pyspark.sql.functions import col, dayofmonth, month, year,
date_format

   

 # Create dataframe for dimDate_gold

   

dfdimDate_gold
=df.dropDuplicates(\["OrderDate"\]).select(col("OrderDate"), \\

         dayofmonth("OrderDate").alias("Day"), \\

         month("OrderDate").alias("Month"), \\

         year("OrderDate").alias("Year"), \\

         date_format(col("OrderDate"), "MMM-yyyy").alias("mmmyyyy"), \\

         date_format(col("OrderDate"), "yyyyMM").alias("yyyymm"), \\

     ).orderBy("OrderDate")

 # Display the first 10 rows of the dataframe to preview your data

display(dfdimDate_gold.head(10))

![A screenshot of a computer Description automatically
generated](./media/image40.png)

![A screenshot of a computer Description automatically
generated](./media/image41.png)

8.  Você está separando o código em novos blocos de código para poder
    entender e observar o que acontece no notebook à medida que
    transforma os dados. Em outro novo bloco de código, **adicione e
    execute o seguinte código** para atualizar a dimensão de data
    conforme novos dados chegam:

> CodeCopy
>
> from delta.tables import \*
>
> deltaTable = DeltaTable.forPath(spark, 'Tables/dimdate_gold')
>
> dfUpdates = dfdimDate_gold
>
> deltaTable.alias('silver') \\
>
> .merge(
>
> dfUpdates.alias('updates'),
>
> 'silver.OrderDate = updates.OrderDate'
>
> ) \\
>
> .whenMatchedUpdate(set =
>
> {
>
> }
>
> ) \\
>
> .whenNotMatchedInsert(values =
>
> {
>
> "OrderDate": "updates.OrderDate",
>
> "Day": "updates.Day",
>
> "Month": "updates.Month",
>
> "Year": "updates.Year",
>
> "mmmyyyy": "updates.mmmyyyy",
>
> "yyyymm": "yyyymm"
>
> }
>
> ) \\
>
> .execute()

![A screenshot of a computer Description automatically
generated](./media/image42.png)

> Sua dimensão de data está toda configurada.

![A screenshot of a computer Description automatically
generated](./media/image43.png)

## **Tarefa 3: Crie sua dimensão de cliente.**

1.  Para criar a tabela de dimensões do cliente, **adicione um novo
    bloco de código**, cole e execute o seguinte código:

> CodeCopy

 from pyspark.sql.types import \*

 from delta.tables import \*

   

 # Create customer_gold dimension delta table

 DeltaTable.createIfNotExists(spark) \\

     .tableName("wwilakehouse.dimcustomer_gold") \\

     .addColumn("CustomerName", StringType()) \\

     .addColumn("Email",  StringType()) \\

     .addColumn("First", StringType()) \\

     .addColumn("Last", StringType()) \\

     .addColumn("CustomerID", LongType()) \\

     .execute()

![A screenshot of a computer Description automatically
generated](./media/image44.png)

![A screenshot of a computer Description automatically
generated](./media/image45.png)

2.  Em um novo bloco de código, **adicione e execute o seguinte código**
    para descartar clientes duplicados, selecionar colunas específicas e
    dividir a coluna “CustomerName” para criar as colunas de nome
    “First” e “Last”:

> CodeCopy
>
> from pyspark.sql.functions import col, split
>
> \# Create customer_silver dataframe
>
> dfdimCustomer_silver =
> df.dropDuplicates(\["CustomerName","Email"\]).select(col("CustomerName"),col("Email"))
> \\
>
> .withColumn("First",split(col("CustomerName"), " ").getItem(0)) \\
>
> .withColumn("Last",split(col("CustomerName"), " ").getItem(1))
>
> \# Display the first 10 rows of the dataframe to preview your data
>
> display(dfdimCustomer_silver.head(10))

![A screenshot of a computer Description automatically
generated](./media/image46.png)

Aqui você criou um novo DataFrame dfdimCustomer_silver, realizando
diversas transformações, como remover duplicatas, selecionar colunas
específicas e dividir a coluna " CustomerName " para criar colunas de
nome "First" e "Last". O resultado é um DataFrame com dados de clientes
organizados e estruturados, incluindo colunas separadas de nome "First"
e "Last" extraídas da coluna " CustomerName ".

![A screenshot of a computer Description automatically
generated](./media/image47.png)

3.  Em seguida, **criaremos** **a coluna ID para nossos clientes.** Em
    um novo bloco de código, cole e execute o seguinte:

CodeCopy

from pyspark.sql.functions import monotonically_increasing_id, col,
when, coalesce, max, lit

\# Read the existing data from the Delta table

dfdimCustomer_temp = spark.read.table("wwilakehouse.dimCustomer_gold")

\# Find the maximum CustomerID or use 0 if the table is empty

MAXCustomerID =
dfdimCustomer_temp.select(coalesce(max(col("CustomerID")),
lit(0)).alias("MAXCustomerID")).first()\[0\]

\# Assume dfdimCustomer_silver is your source DataFrame with new data

\# Here, we select only the new customers by doing a left anti join

dfdimCustomer_gold = dfdimCustomer_silver.join(

    dfdimCustomer_temp,

    (dfdimCustomer_silver.CustomerName ==
dfdimCustomer_temp.CustomerName) &

    (dfdimCustomer_silver.Email == dfdimCustomer_temp.Email),

    "left_anti"

)

\# Add the CustomerID column with unique values starting from
MAXCustomerID + 1

dfdimCustomer_gold = dfdimCustomer_gold.withColumn(

    "CustomerID",

    monotonically_increasing_id() + MAXCustomerID + 1

)

\# Display the first 10 rows of the dataframe to preview your data

dfdimCustomer_gold.show(10)

![](./media/image48.png)

![A screenshot of a computer Description automatically
generated](./media/image49.png)

4.  Agora você garantirá que sua tabela de clientes permaneça atualizada
    conforme novos dados chegam. **Em um novo bloco de código**, cole e
    execute o seguinte:

> CodeCopy

from delta.tables import DeltaTable

\# Define the Delta table path

deltaTable = DeltaTable.forPath(spark, 'Tables/dimcustomer_gold')

\# Use dfUpdates to refer to the DataFrame with new or updated records

dfUpdates = dfdimCustomer_gold

\# Perform the merge operation to update or insert new records

deltaTable.alias('silver') \\

  .merge(

    dfUpdates.alias('updates'),

    'silver.CustomerName = updates.CustomerName AND silver.Email =
updates.Email'

  ) \\

  .whenMatchedUpdate(set =

    {

      "CustomerName": "updates.CustomerName",

      "Email": "updates.Email",

      "First": "updates.First",

      "Last": "updates.Last",

      "CustomerID": "updates.CustomerID"

    }

  ) \\

  .whenNotMatchedInsert(values =

    {

      "CustomerName": "updates.CustomerName",

      "Email": "updates.Email",

      "First": "updates.First",

      "Last": "updates.Last",

      "CustomerID": "updates.CustomerID"

    }

  ) \\

  .execute()

![](./media/image50.png)

![A screenshot of a computer Description automatically
generated](./media/image51.png)

5.  Agora você **repetirá esses passos para criar a dimensão do seu
    produto**. Em um novo bloco de código, cole e execute o seguinte:

> CodeCopy
>
> from pyspark.sql.types import \*
>
> from delta.tables import \*
>
> DeltaTable.createIfNotExists(spark) \\
>
> .tableName("wwilakehouse.dimproduct_gold") \\
>
> .addColumn("ItemName", StringType()) \\
>
> .addColumn("ItemID", LongType()) \\
>
> .addColumn("ItemInfo", StringType()) \\
>
> .execute()

![A screenshot of a computer Description automatically
generated](./media/image52.png)

![A screenshot of a computer Description automatically
generated](./media/image53.png)

6.  **Adicione outro bloco de código** para criar o dataframe
    **product_silver**.

> CodeCopy
>
> from pyspark.sql.functions import col, split, lit
>
> \# Create product_silver dataframe
>
> dfdimProduct_silver =
> df.dropDuplicates(\["Item"\]).select(col("Item")) \\
>
> .withColumn("ItemName",split(col("Item"), ", ").getItem(0)) \\
>
> .withColumn("ItemInfo",when((split(col("Item"), ",
> ").getItem(1).isNull() | (split(col("Item"), ",
> ").getItem(1)=="")),lit("")).otherwise(split(col("Item"), ",
> ").getItem(1)))
>
> \# Display the first 10 rows of the dataframe to preview your data
>
> display(dfdimProduct_silver.head(10))

![A screenshot of a computer Description automatically
generated](./media/image54.png)

![A screenshot of a computer Description automatically
generated](./media/image55.png)

7.  Agora você criará IDs para sua **tabela dimProduct_gold**. Adicione
    a seguinte sintaxe a um novo bloco de código e execute-o:

CodeCopy

from pyspark.sql.functions import monotonically_increasing_id, col, lit,
max, coalesce

\#dfdimProduct_temp = dfdimProduct_silver

dfdimProduct_temp = spark.read.table("wwilakehouse.dimProduct_gold")

MAXProductID =
dfdimProduct_temp.select(coalesce(max(col("ItemID")),lit(0)).alias("MAXItemID")).first()\[0\]

dfdimProduct_gold =
dfdimProduct_silver.join(dfdimProduct_temp,(dfdimProduct_silver.ItemName
== dfdimProduct_temp.ItemName) & (dfdimProduct_silver.ItemInfo ==
dfdimProduct_temp.ItemInfo), "left_anti")

dfdimProduct_gold =
dfdimProduct_gold.withColumn("ItemID",monotonically_increasing_id() +
MAXProductID + 1)

\# Display the first 10 rows of the dataframe to preview your data

display(dfdimProduct_gold.head(10))

![A screenshot of a computer Description automatically
generated](./media/image56.png)

Isso calcula o próximo ID de produto disponível com base nos dados
atuais na tabela, atribui esses novos IDs aos produtos e, em seguida,
exibe as informações atualizadas do produto.

![A screenshot of a computer Description automatically
generated](./media/image57.png)

8.  Semelhante ao que você fez com suas outras dimensões, você precisa
    garantir que sua tabela de produtos permaneça atualizada conforme
    novos dados chegam. **Em um novo bloco de código**, cole e execute o
    seguinte:

CodeCopy

from delta.tables import \*

deltaTable = DeltaTable.forPath(spark, 'Tables/dimproduct_gold')

dfUpdates = dfdimProduct_gold

deltaTable.alias('silver') \\

.merge(

dfUpdates.alias('updates'),

'silver.ItemName = updates.ItemName AND silver.ItemInfo =
updates.ItemInfo'

) \\

.whenMatchedUpdate(set =

{

}

) \\

.whenNotMatchedInsert(values =

{

"ItemName": "updates.ItemName",

"ItemInfo": "updates.ItemInfo",

"ItemID": "updates.ItemID"

}

) \\

.execute()

![A screenshot of a computer Description automatically
generated](./media/image58.png)

![A screenshot of a computer Description automatically
generated](./media/image59.png)

**Agora que as dimensões foram criadas, a etapa final é criar a tabela
de fatos.**

9.  **Em um novo bloco de código** cole e execute o seguinte código para
    criar a **tabela de fatos**:

> CodeCopy
>
> from pyspark.sql.types import \*
>
> from delta.tables import \*
>
> DeltaTable.createIfNotExists(spark) \\
>
> .tableName("wwilakehouse.factsales_gold") \\
>
> .addColumn("CustomerID", LongType()) \\
>
> .addColumn("ItemID", LongType()) \\
>
> .addColumn("OrderDate", DateType()) \\
>
> .addColumn("Quantity", IntegerType()) \\
>
> .addColumn("UnitPrice", FloatType()) \\
>
> .addColumn("Tax", FloatType()) \\
>
> .execute()

![A screenshot of a computer Description automatically
generated](./media/image60.png)

![A screenshot of a computer Description automatically
generated](./media/image61.png)

10. **Em um novo bloco de código** cole e execute o seguinte código para
    criar um **novo dataframe** para combinar dados de vendas com
    informações do cliente e do produto, incluindo ID do cliente, ID do
    item, data do pedido, quantidade, preço unitário e imposto:

CodeCopy

from pyspark.sql import SparkSession

from pyspark.sql.functions import split, col, when, lit

from pyspark.sql.types import StructType, StructField, StringType,
IntegerType, DateType, FloatType, BooleanType, TimestampType

\# Initialize Spark session

spark = SparkSession.builder \\

    .appName("DeltaTableUpsert") \\

    .config("spark.sql.extensions",
"io.delta.sql.DeltaSparkSessionExtension") \\

    .config("spark.sql.catalog.spark_catalog",
"org.apache.spark.sql.delta.catalog.DeltaCatalog") \\

    .getOrCreate()

\# Define the schema for the sales_silver table

silver_table_schema = StructType(\[

    StructField("SalesOrderNumber", StringType(), True),

    StructField("SalesOrderLineNumber", IntegerType(), True),

    StructField("OrderDate", DateType(), True),

    StructField("CustomerName", StringType(), True),

    StructField("Email", StringType(), True),

    StructField("Item", StringType(), True),

    StructField("Quantity", IntegerType(), True),

    StructField("UnitPrice", FloatType(), True),

    StructField("Tax", FloatType(), True),

    StructField("FileName", StringType(), True),

    StructField("IsFlagged", BooleanType(), True),

    StructField("CreatedTS", TimestampType(), True),

    StructField("ModifiedTS", TimestampType(), True)

\])

\# Define the path to the Delta table (ensure this path is correct)

delta_table_path =
"abfss://\<container\>@\<storage-account\>.dfs.core.windows.net/path/to/wwilakehouse/sales_silver"

\# Create a DataFrame with the defined schema

empty_df = spark.createDataFrame(\[\], silver_table_schema)

\# Register the Delta table in the Metastore

spark.sql(f"""

    CREATE TABLE IF NOT EXISTS wwilakehouse.sales_silver

    USING DELTA

    LOCATION '{delta_table_path}'

""")

\# Load data into DataFrame

df = spark.read.table("wwilakehouse.sales_silver")

\# Perform transformations on df

df = df.withColumn("ItemName", split(col("Item"), ", ").getItem(0)) \\

    .withColumn("ItemInfo", when(

        (split(col("Item"), ", ").getItem(1).isNull()) |
(split(col("Item"), ", ").getItem(1) == ""),

        lit("")

    ).otherwise(split(col("Item"), ", ").getItem(1)))

\# Load additional DataFrames for joins

dfdimCustomer_temp = spark.read.table("wwilakehouse.dimCustomer_gold")

dfdimProduct_temp = spark.read.table("wwilakehouse.dimProduct_gold")

\# Create Sales_gold dataframe

dffactSales_gold = df.alias("df1").join(dfdimCustomer_temp.alias("df2"),
(df.CustomerName == dfdimCustomer_temp.CustomerName) & (df.Email ==
dfdimCustomer_temp.Email), "left") \\

    .join(dfdimProduct_temp.alias("df3"), (df.ItemName ==
dfdimProduct_temp.ItemName) & (df.ItemInfo ==
dfdimProduct_temp.ItemInfo), "left") \\

    .select(

        col("df2.CustomerID"),

        col("df3.ItemID"),

        col("df1.OrderDate"),

        col("df1.Quantity"),

        col("df1.UnitPrice"),

        col("df1.Tax")

    ).orderBy(col("df1.OrderDate"), col("df2.CustomerID"),
col("df3.ItemID"))

\# Show the result

dffactSales_gold.show()

![A screenshot of a computer Description automatically
generated](./media/image62.png)

![A screenshot of a computer Description automatically
generated](./media/image63.png)

![A screenshot of a computer Description automatically
generated](./media/image64.png)

1.  Agora você garantirá que os dados de vendas permaneçam atualizados
    executando o seguinte código em um **novo bloco de código** :

> CodeCopy
>
> from delta.tables import \*
>
> deltaTable = DeltaTable.forPath(spark, 'Tables/factsales_gold')
>
> dfUpdates = dffactSales_gold
>
> deltaTable.alias('silver') \\
>
> .merge(
>
> dfUpdates.alias('updates'),
>
> 'silver.OrderDate = updates.OrderDate AND silver.CustomerID =
> updates.CustomerID AND silver.ItemID = updates.ItemID'
>
> ) \\
>
> .whenMatchedUpdate(set =
>
> {
>
> }
>
> ) \\
>
> .whenNotMatchedInsert(values =
>
> {
>
> "CustomerID": "updates.CustomerID",
>
> "ItemID": "updates.ItemID",
>
> "OrderDate": "updates.OrderDate",
>
> "Quantity": "updates.Quantity",
>
> "UnitPrice": "updates.UnitPrice",
>
> "Tax": "updates.Tax"
>
> }
>
> ) \\
>
> .execute()

![A screenshot of a computer Description automatically
generated](./media/image65.png)

Aqui, você usa a operação de mesclagem do Delta Lake para sincronizar e
atualizar a tabela factsales_gold com novos dados de vendas
(dffactSales_gold). A operação compara a data do pedido, o ID do cliente
e o ID do item entre os dados existentes (tabela Silver) e os novos
dados (atualiza o DataFrame), atualizando os registros correspondentes e
inserindo novos registros conforme necessário.

![A screenshot of a computer Description automatically
generated](./media/image66.png)

Agora, você tem uma camada **gold** curada e modelada, que pode ser
usada para relatórios e análise.

# Exercício 4: Estabelecendo conectividade entre o Azure Databricks e o Azure Data Lake Storage (ADLS) Gen 2

Agora, vamos criar uma tabela Delta com a ajuda da sua conta do Azure
Data Lake Storage (ADLS) Gen2 usando o Azure Databricks. Em seguida,
você criará um atalho do OneLake para uma tabela Delta no ADLS e usará o
Power BI para analisar dados por meio do atalho do ADLS.

## **Tarefa 0: Resgatar um Azure Pass e ativar a assinatura do Azure**

1.  Navegue no link a seguir !!https://www.microsoftazurepass.com/!! e
    clique no botão **Start**.

![](./media/image67.png)

2.  Na página de login da Microsoft, insira o **ID do locatário e**
    clique em **Next**.

![](./media/image68.png)

3.  Na próxima página, digite sua senha e clique em **Sign In**.

![](./media/image69.png)

![A screenshot of a computer error Description automatically
generated](./media/image70.png)

4.  Após efetuar login, na página do Microsoft Azure, clique na aba
    **Confirm Microsoft Account**.

![](./media/image71.png)

5.  Na próxima página, insira o código promocional, os caracteres do
    Captcha e clique em **Submit.**

![A screenshot of a computer Description automatically
generated](./media/image72.png)

![A screenshot of a computer error Description automatically
generated](./media/image73.png)

6.  Na página Your profile, insira os detalhes do seu perfil e clique em
    **Sign up.**

7.  se solicitado, inscreva-se para autenticação multifator e, em
    seguida, faça login no portal do Azure navegando até o link a
    seguir!! <https://portal.azure.com/#home> !!

![](./media/image74.png)

8.  Na barra de pesquisa, digite Assinatura e clique no ícone
    Subscription em **Services.**

![A screenshot of a computer Description automatically
generated](./media/image75.png)

9.  Após o resgate bem-sucedido do **Azure pass**, um ID de assinatura
    será gerado.

![](./media/image76.png)

## **Tarefa 1: Criar uma conta de armazenamento de dados do Azure**

1.  Entre no seu portal do Azure usando suas credenciais do Azure.

2.  Na página inicial, no menu do portal à esquerda, selecione **Storage
    accounts** para exibir uma lista das suas contas de armazenamento.
    Se o menu do portal não estiver visível, selecione o botão de menu
    para ativá-lo.

![A screenshot of a computer Description automatically
generated](./media/image77.png)

3.  Na página **Storage accounts**, selecione **Create**.

![A screenshot of a computer Description automatically
generated](./media/image78.png)

4.  Na aba Basics, ao selecionar um grupo de recursos, forneça as
    informações essenciais para sua conta de armazenamento:

[TABLE]

Deixe as outras configurações como estão e selecione **Review + create**
para aceitar as opções padrões e prosseguir para validar e criar a
conta.

Observação: se você ainda não tiver um grupo de recursos criado, clique
em “**Create new**” e crie um novo recurso para sua conta de
armazenamento.

![](./media/image79.png)

![](./media/image80.png)

![A screenshot of a computer Description automatically
generated](./media/image81.png)

![A screenshot of a computer Description automatically
generated](./media/image82.png)

![A screenshot of a computer Description automatically
generated](./media/image83.png)

5.  Ao navegar até a aba **Review + create**, o Azure executa a
    validação nas configurações da conta de armazenamento que você
    escolheu. Se a validação for bem-sucedida, você poderá prosseguir
    com a criação da conta de armazenamento.

Se a validação falhar, o portal indicará quais configurações precisam
ser modificadas.

![A screenshot of a computer Description automatically
generated](./media/image84.png)

![A screenshot of a computer Description automatically
generated](./media/image85.png)

Agora você criou com sucesso sua conta de armazenamento de dados do
Azure.

6.  Navegue até a página de contas de armazenamento pesquisando na barra
    de pesquisa na parte superior da página e selecione a conta de
    armazenamento recém-criada.

![A screenshot of a computer Description automatically
generated](./media/image86.png)

7.  Na página da conta de armazenamento, navegue até **Containers** em
    **Data storage** no painel de navegação esquerdo, crie um novo
    contêiner com o nome !!medalion1!! e clique no botão **Create**.

 

![A screenshot of a computer Description automatically
generated](./media/image87.png)

8.  Agora, volte para a página da **storage account**, selecione
    **Endpoints** no menu de navegação à esquerda. Role para baixo,
    copie a **Primary endpoint URL** e cole- a em um bloco de notas.
    Isso será útil ao criar o atalho.

![](./media/image88.png)

9.  Da mesma forma, navegue até as **Access keys** no mesmo painel de
    navegação.

![A screenshot of a computer Description automatically
generated](./media/image89.png)

## **Tarefa 2: Crie uma tabela Delta, crie um atalho e analise os dados em seu Lakehouse**

1.  No seu lakehouse, selecione as reticências **(…)** ao lado dos
    arquivos e então selecione **New shortcut**.

![](./media/image90.png)

2.  Na tela **New shortcut**, selecione o **Azure Data Lake Storage
    Gen2**.

![Screenshot of the tile options in the New shortcut
screen.](./media/image91.png)

3.  Especifique os detalhes de conexão para o atalho:

[TABLE]

4.  E clique em **Next**.

![A screenshot of a computer Description automatically
generated](./media/image92.png)

5.  Isso estabelecerá um link com seu contêiner de armazenamento do
    Azure. Selecione o armazenamento e clique no botão **Next**.

![A screenshot of a computer Description automatically
generated](./media/image93.png)

![A screenshot of a computer Description automatically
generated](./media/image94.png)![A screenshot of a computer Description
automatically generated](./media/image95.png)

6.  Depois que o Assistente for iniciado, selecione **Files** e
    selecione **“…“** no arquivo **bronze** .

![A screenshot of a computer Description automatically
generated](./media/image96.png)

7.  Selecione **load to tables** e **new table**.

![](./media/image97.png)

8.  Na janela pop-up, nomeie sua tabela como **bronze_01** e selecione o
    tipo de arquivo como **parquet** .

![A screenshot of a computer Description automatically
generated](./media/image98.png)

![A screenshot of a computer Description automatically
generated](./media/image99.png)

![A screenshot of a computer Description automatically
generated](./media/image100.png)

![A screenshot of a computer Description automatically
generated](./media/image101.png)

9.  O arquivo **bronze_01** agora está visível nos arquivos.

![A screenshot of a computer Description automatically
generated](./media/image102.png)

10. Em seguida, selecione **“…”** no arquivo **bronze.** Selecione
    **load to tables** e **existing table.**

![A screenshot of a computer Description automatically
generated](./media/image103.png)

11. Forneça o nome da tabela existente como **dimcustomer_gold.**
    Selecione o tipo de arquivo como **parquet** e selecione **load.**

![A screenshot of a computer Description automatically
generated](./media/image104.png)

![A screenshot of a computer Description automatically
generated](./media/image105.png)

## **Tarefa 3: Criar um modelo semântico usando a camada gold para criar um relatório**

No seu workspace, agora você pode usar a camada Gold para criar um
relatório e analisar os dados. Você pode acessar o modelo semântico
diretamente no seu workspace para criar relacionamentos e medidas para
relatórios.

*Observe que você não pode usar o **modelo semântico padrão,** criado
automaticamente ao criar um lakehouse. Você deve criar um novo modelo
semântico que inclua as tabelas de Gold que você criou neste
laboratório, a partir do explorador de lakehouses.*

1.  No seu workspace, navegue até o seu **wwilakehouse** lakehouse. Em
    seguida, selecione **New semantic model** na faixa de opções da
    visualização do explorador do lakehouse.

![A screenshot of a computer Description automatically
generated](./media/image106.png)

2.  No pop-up, atribua o nome **DatabricksTutorial** ao seu novo modelo
    semântico e selecione o workspace como **Fabric Lakehouse
    Tutorial-29**.

![](./media/image107.png)

3.  Em seguida, role para baixo e selecione tudo para incluir no seu
    modelo semântico e selecione **Confirm**.

Isso abrirá o modelo semântico no Fabric, onde você pode criar relações
e medidas, como mostrado aqui:

![A screenshot of a computer Description automatically
generated](./media/image108.png)

A partir daqui, você ou outros membros da sua equipe de dados podem
criar relatórios e painéis com base nos dados do seu lakehouse. Esses
relatórios serão conectados diretamente à camada Gold do seu lakehouse,
para que sempre reflitam os dados mais recentes.

# Exercício 5: Ingestão de dados e análise com o Azure Databricks

1.  Navegue até seu lakehouse no serviço Power BI e selecione **Get
    data** e, em seguida, **New data pipeline**.

![Screenshot showing how to navigate to new data pipeline option from
within the UI.](./media/image109.png)

2.  No prompt **New pipeline**, insira um nome para o novo pipeline e
    selecione **Create**. **IngestDatapipeline01**

![](./media/image110.png)

3.  Para este exercício, selecione os dados de exemplo **NYC Taxi -
    Green** como fonte de dados.

![A screenshot of a computer Description automatically
generated](./media/image111.png)

4.  Na tela de visualização, selecione **Next**.

![A screenshot of a computer Description automatically
generated](./media/image112.png)

5.  Para o destino dos dados, selecione o nome da tabela que deseja usar
    para armazenar os dados da tabela Delta do OneLake. Você pode
    escolher uma tabela existente ou criar uma nova. Para este
    laboratório, selecione **load into new table** e **Next** .

![A screenshot of a computer Description automatically
generated](./media/image113.png)

6.  Na tela **Review + Save**, selecione **Start data transfer
    immediately** e depois selecione **Save + Run**.

![Screenshot showing how to enter table name.](./media/image114.png)

![A screenshot of a computer Description automatically
generated](./media/image115.png)

7.  Quando o trabalho estiver concluído, navegue até seu lakehouse e
    visualize a tabela delta listada em /Tabelas.

![A screenshot of a computer Description automatically
generated](./media/image116.png)

8.  Copie o caminho do Azure Blob Filesystem (ABFS) para sua tabela
    delta clicando com o botão direito do mouse no nome da tabela na
    exibição do Explorer e selecionando **Properties**.

![A screenshot of a computer Description automatically
generated](./media/image117.png)

9.  Abra seu notebook do Azure Databricks e execute o código.

olsPath = "**abfss://\<replace with workspace
name\>@onelake.dfs.fabric.microsoft.com/\<replace with item
name\>.Lakehouse/Tables/nycsample**"

df=spark.read.format('delta').option("inferSchema","true").load(olsPath)

df.show(5)

*Observação: substitua o caminho do arquivo em negrito pelo que você
copiou.*

![](./media/image118.png)

10. Atualize os dados da tabela Delta alterando um valor de campo.

%sql

update delta.\`abfss://\<replace with workspace
name\>@onelake.dfs.fabric.microsoft.com/\<replace with item
name\>.Lakehouse/Tables/nycsample\` set vendorID = 99999 where vendorID
= 1;

*Observação: substitua o caminho do arquivo em negrito pelo que você
copiou.*

![A screenshot of a computer Description automatically
generated](./media/image119.png)

# Exercício 6: Limpar recursos

Neste exercício, você aprendeu a criar uma arquitetura medallion em um
lakehouse do Microsoft Fabric.

Quando você terminar de explorar seu lakehouse, poderá excluir o
workspace que criou para este exercício.

1.  Selecione seu workspace, o **Fabric Lakehouse Tutorial-29**, no menu
    de navegação à esquerda. Isso abrirá a visualização de itens do
    workspace.

![A screenshot of a computer Description automatically
generated](./media/image120.png)

2.  Selecione a opção ***...*** sob o nome do workspace e selecione
    **Workspace settings**.

![A screenshot of a computer Description automatically
generated](./media/image121.png)

3.  Role para baixo até o final e **Remove this workspace.**

![A screenshot of a computer Description automatically
generated](./media/image122.png)

4.  Clique em **Delete** no aviso que aparece.

![A white background with black text Description automatically
generated](./media/image123.png)

5.  Aguarde uma notificação de que o Workspace foi excluído antes de
    prosseguir para o próximo laboratório.

![A screenshot of a computer Description automatically
generated](./media/image124.png)

**Resumo** :

Este laboratório orienta os participantes na criação de uma arquitetura
Medallion em um Lakehouse do Microsoft Fabric usando notebooks. As
principais etapas incluem a configuração de um workspace, o
estabelecimento de um Lakehouse, o carregamento de dados para a camada
Bronze para ingestão inicial, a transformação em uma tabela Delta Silver
para processamento estruturado, o refinamento posterior em tabelas Delta
Gold para análises avançadas, a exploração de modelos semânticos e a
criação da relação de dados para análises perspicazes.

## 
