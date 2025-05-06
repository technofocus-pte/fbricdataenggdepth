**Introdução**

O Apache Spark é um mecanismo de código aberto para processamento
distribuído de dados, amplamente utilizado para explorar, processar e
analisar grandes volumes de dados em data lakes. O Spark está disponível
como opção de processamento em diversos produtos de plataforma de dados,
incluindo Azure HDInsight, Azure Databricks, Azure Synapse Analytics e
Microsoft Fabric. Um dos benefícios do Spark é o suporte a uma ampla
gama de linguagens de programação, incluindo Java, Scala, Python e SQL,
tornando o Spark uma solução muito flexível para cargas de trabalho de
processamento de dados, incluindo limpeza e manipulação de dados,
análise estatística e aprendizado de máquina, além de análise e
visualização de dados.

As tabelas em um lakehouse do Microsoft Fabric são baseadas no código
aberto Formato *Delta Lake* para Apache Spark. O Delta Lake adiciona
suporte à semântica relacional para operações de dados em lote e
streaming e permite a criação de uma arquitetura Lakehouse na qual o
Apache Spark pode ser usado para processar e consultar dados em tabelas
baseadas em arquivos subjacentes em um data lake.

No Microsoft Fabric, os Fluxos de Dados (Gen2) se conectam a várias
fontes de dados e realizam transformações no Power Query Online. Eles
podem ser usados em Pipelines de Dados para inserir dados em um
lakehouse ou outro armazenamento analítico, ou para definir um conjunto
de dados para um relatório do Power BI.

Este laboratório foi projetado para apresentar os diferentes elementos
dos Dataflows (Gen2), e não criar uma solução complexa que possa existir
em uma empresa.

**Objetivos** :

- Crie um workspace no Microsoft Fabric com a versão de avaliação do
  Fabric habilitada.

- Estabeleça um ambiente de lakehouse e carregue arquivos de dados para
  análise.

- Gere um notebook para exploração e análise interativa de dados.

- Carregue os dados em um dataframe para posterior processamento e
  visualização.

- Aplique transformações aos dados usando o PySpark .

- Salve e participe dos dados transformados para otimizar as consultas.

- Crie uma tabela no metastore do Spark para gerenciamento de dados
  estruturados

- Salve o DataFrame como uma tabela delta gerenciada chamada "
  salesorders".

- Salve o DataFrame como uma tabela delta externa chamada
  "external_salesorder" com um caminho específico.

- Descreva e compare propriedades de tabelas gerenciadas e externas.

- Execute consultas SQL em tabelas para análise e geração de relatórios.

- Visualize dados usando bibliotecas Python, como matplotlib e seaborn.

- Estabeleça um data lakehouse na experiência de Engenharia de Dados e
  insira dados relevantes para análise subsequente.

- Defina um fluxo de dados para extrair, transformar e carregar dados no
  lakehouse .

- Configure destinos de dados no Power Query para armazenar os dados
  transformados no lakehouse .

- Incorporar o fluxo de dados em um pipeline para permitir o
  processamento e a inserção de dados programados.

- Remova o workspace e os elementos associados para concluir o
  exercício.

# Exercício 1: Crie um workspace, um lakehouse , um notebook e carregue os dados no dataframe 

## Tarefa 1: Criar um workspace

Antes de trabalhar com dados no Fabric, crie um workspace com o teste do
Fabric habilitado.

1.  Abra seu navegador, navegue até a barra de endereço e digite ou cole
    o seguinte URL: <https://app.fabric.microsoft.com/> então pressione
    o botão **Enter .**

> **Observação** : se você for direcionado para a página inicial do
> Microsoft Fabric, pule as etapas de 2 a 4.
>
> ![](./media/image1.png)

2.  Na janela **do Microsoft Fabric** ,insira suas credenciais e clique
    no botão **Submit**.

> ![](./media/image2.png)

3.  Em seguida, na janela da **Microsoft**, digite a senha e clique no
    botão **Sign in.**

> ![A login screen with a red box and blue text Description
> automatically generated](./media/image3.png)

4.  Na janela **Stay signed in?,** clique no botão **Yes**.

> ![A screenshot of a computer error Description automatically
> generated](./media/image4.png)

5.  Página inicial do Fabric , selecione **+New workspace**.

> ![A screenshot of a computer AI-generated content may be
> incorrect.](./media/image5.png)

6.  Na **aba Create a workspace**, insira os seguintes detalhes e clique
    no botão **Apply**.

[TABLE]

> ![A screenshot of a computer Description automatically
> generated](./media/image6.png)
>
> ![](./media/image7.png)

![](./media/image8.png)

![](./media/image9.png)

7.  Aguarde a conclusão da implementação. Leva de 2 a 3 minutos. Quando
    o seu novo workspace for aberto, ele deverá estar vazio.

## Tarefa 2: Criar um lakehouse e enviar arquivos

Agora que você tem um workspace, é hora de mudar para a experiência *de
Engenharia de dados* no portal e criar um data lakehouse para os
arquivos de dados que você vai analisar.

1.  Crie um novo Eventhouse clicando no botão **+New item** na barra de
    navegação.

![A screenshot of a browser AI-generated content may be
incorrect.](./media/image10.png)

2.  Clique em "**Lakehouse**".

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image11.png)

3.  Na caixa de diálogo **New lakehouse**, digite
    **+++Fabric_lakehouse+++** no campo **Nome**, clique no botão
    **Create** e abra o novo lakehouse .

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image12.png)

4.  Após cerca de um minuto, um novo lakehouse vazio será criado. Você
    precisará inserir alguns dados no data lakehouse para análise.

![](./media/image13.png)

5.  Você verá uma notificação informando **Successfully created SQL
    endpoint**.

![](./media/image14.png)

6.  Na seção **Explorer**, em **fabric_lakehouse**, passe o mouse sobre
    **Files folder** e clique no menu de reticências horizontais **(…)**
    . Navegue e clique em **Upload** .Em seguida, clique na **Upload
    folder**, conforme mostrado na imagem abaixo.

![](./media/image15.png)

7.  No painel **Upload folder** que aparece no lado direito, selecione o
    **folder icon** em **Files/** e navegue até **C:\LabFiles** e
    selecione a pasta **orders** e clique no botão **Upload**.

![](./media/image16.png)

8.  Caso a caixa de diálogo **Upload 3 files to this site**? apareça,
    clique no botão **Upload**.

![](./media/image17.png)

9.  No painel Upload folder, clique no botão **Upload** .

> ![](./media/image18.png)

10. Depois que os arquivos forem carregados, **close** o painel **Upload
    folder**.

> ![](./media/image19.png)

11. Expanda **Files**, selecione a pasta **orders** e verifique se os
    arquivos CSV foram carregados.

![](./media/image20.png)

## Tarefa 3: Criar um notebook

Para trabalhar com dados no Apache Spark, você pode criar um *notebook*
. Os notebooks oferecem um ambiente interativo no qual você pode
escrever e executar código (em várias linguagens) e adicionar notas para
documentá-lo.

1.  Na página **Home**, enquanto visualiza o conteúdo da pasta
    **orders** no seu datalake , no menu **Open notebook**, selecione
    **New notebook**.

![](./media/image21.png)

2.  Após alguns segundos, um novo notebook contendo uma única *célula*
    será aberto. Notebooks são compostos por uma ou mais células que
    podem conter *código* ou *markdown* (texto formatado).

![](./media/image22.png)

3.  Selecione a primeira célula (que atualmente é uma célula *de
    código*) e, na barra de ferramentas dinâmica no canto superior
    direito, use o botão **M↓ para converter a célula em uma célula de
    markdown** .

![](./media/image23.png)

4.  Quando a célula muda para uma célula markdown, o texto que ela
    contém é processado.

![](./media/image24.png)

5.  Use o botão **🖉** (Edit) para alternar a célula para o modo de
    edição, substitua todo o texto e modifique o markdown da seguinte
    forma:

> CodeCopy
>
> \# Sales order data exploration
>
> Use the code in this notebook to explore sales order data.

![](./media/image25.png)

![A screenshot of a computer Description automatically
generated](./media/image26.png)

6.  Clique em qualquer lugar do notebook fora da célula para parar de
    editá-lo e ver a marcação processada.

![A screenshot of a computer Description automatically
generated](./media/image27.png)

## Tarefa 4: Carregar dados em um dataframe

Agora você está pronto para executar o código que carrega os dados em um
*dataframe* .Os dataframes no Spark são semelhantes aos dataframes do
Pandas em Python e fornecem uma estrutura comum para trabalhar com dados
em linhas e colunas.

**Observação** :O Spark suporta diversas linguagens de programação,
incluindo Scala, Java e outras. Neste exercício, usaremos *o PySpark* ,
uma variante do Python otimizada para Spark. O PySpark é uma das
linguagens mais usadas no Spark e é a linguagem padrão nos notebooks do
Fabric.

1.  Com o notebook visível, expanda a lista **Files** e selecione a
    pasta **orders** para que os arquivos CSV sejam listados ao lado do
    editor de notebook.

![](./media/image28.png)

2.  Agora, mova o cursor do mouse até o arquivo 2019.csv. Clique nas
    reticências horizontais **(...)** ao lado de 2019.csv. Navegue e
    clique em **Load data** e selecione **Spark**. Uma nova célula de
    código contendo o seguinte código será adicionada ao notebook:

> CodeCopy
>
> df =
> spark.read.format("csv").option("header","true").load("Files/orders/2019.csv")
>
> \# df now is a Spark DataFrame containing CSV data from
> "Files/orders/2019.csv".
>
> display(df)

![](./media/image29.png)

![](./media/image30.png)

**Dica** : Você pode ocultar os painéis do Lakehouse Explorer à esquerda
usando seus ícones **«.** azer isso ajudará você a se concentrar melhor
no notebook.

3.  Use o botão **▷ Run cell** à esquerda da célula para executá-la.

![](./media/image31.png)

**Observação** : Como esta é a primeira vez que você executa um código
Spark, uma sessão Spark precisa ser iniciada. Isso significa que a
primeira execução da sessão pode levar cerca de um minuto para ser
concluída. As execuções subsequentes serão mais rápidas.

4.  Quando o comando da célula for concluído, revise a saída abaixo da
    célula, que deve ser semelhante a esta:

![](./media/image32.png)

5.  A saída mostra as linhas e colunas de dados do arquivo 2019.csv. No
    entanto, observe que os cabeçalhos das colunas não parecem corretos.
    O código padrão usado para carregar os dados em um dataframe
    pressupõe que o arquivo CSV inclui os nomes das colunas na primeira
    linha, mas, neste caso, o arquivo CSV inclui apenas os dados, sem
    informações de cabeçalho.

6.  Modifique o código para definir a opção de **cabeçalho** como
    **falsa**. Substitua todo o código na **célula** pelo seguinte
    código e clique no botão **▷ Run cell** e revise a saída.

> CodeCopy
>
> df =
> spark.read.format("csv").option("header","false").load("Files/orders/2019.csv")
>
> \# df now is a Spark DataFrame containing CSV data from
> "Files/orders/2019.csv".
>
> display(df)
>
> ![](./media/image33.png)

7.  Agora, o dataframe inclui corretamente a primeira linha como valores
    de dados, mas os nomes das colunas são gerados automaticamente e não
    são muito úteis. Para entender os dados, você precisa definir
    explicitamente o esquema e o tipo de dados corretos para os valores
    de dados no arquivo.

8.  Substitua todo o código na **célula** pelo seguinte código e clique
    no botão **▷ Run cell** e revise a saída

> CodeCopy
>
> from pyspark.sql.types import \*
>
> orderSchema = StructType(\[
>
> StructField("SalesOrderNumber", StringType()),
>
> StructField("SalesOrderLineNumber", IntegerType()),
>
> StructField("OrderDate", DateType()),
>
> StructField("CustomerName", StringType()),
>
> StructField("Email", StringType()),
>
> StructField("Item", StringType()),
>
> StructField("Quantity", IntegerType()),
>
> StructField("UnitPrice", FloatType()),
>
> StructField("Tax", FloatType())
>
> \])
>
> df =
> spark.read.format("csv").schema(orderSchema).load("Files/orders/2019.csv")
>
> display(df)

![](./media/image34.png)

> ![](./media/image35.png)

9.  Agora, o dataframe inclui os nomes de coluna corretos (além do
    **Index**, que é uma coluna integrada em todos os dataframes com
    base na posição ordinal de cada linha). Os tipos de dados das
    colunas são especificados usando um conjunto padrão de tipos
    definidos na biblioteca Spark SQL, que foram importados no início da
    célula.

10. Confirme se suas alterações foram aplicadas aos dados visualizando o
    dataframe .

11. Use o ícone **+Code** abaixo da saída da célula para adicionar uma
    nova célula de código ao notebook e insira o seguinte código nela.
    Clique no botão **▷ Run cell** e revise a saída.

> CodeCopy
>
> display(df)
>
> ![A screenshot of a computer Description automatically
> generated](./media/image36.png)

12. O dataframe inclui apenas os dados do arquivo **2019.csv**.
    Modifique o código para que o caminho do arquivo use um curinga \*
    para ler os dados do pedido de vendas de todos os arquivos na pasta
    **orders.**

13. Use o ícone **+ Code** abaixo da saída da célula para adicionar uma
    nova célula de código ao notebook e insira o seguinte código nela.

CodeCopy

> from pyspark.sql.types import \*
>
> orderSchema = StructType(\[
>
>     StructField("SalesOrderNumber", StringType()),
>
>     StructField("SalesOrderLineNumber", IntegerType()),
>
>     StructField("OrderDate", DateType()),
>
>     StructField("CustomerName", StringType()),
>
>     StructField("Email", StringType()),
>
>     StructField("Item", StringType()),
>
>     StructField("Quantity", IntegerType()),
>
>     StructField("UnitPrice", FloatType()),
>
>     StructField("Tax", FloatType())
>
>     \])
>
> df =
> spark.read.format("csv").schema(orderSchema).load("Files/orders/\*.csv")

display(df)

![](./media/image37.png)

14. Execute a célula de código modificada e revise a saída, que agora
    deve incluir as vendas de 2019, 2020 e 2021.

![](./media/image38.png)

**Observação** : Sum subconjunto das linhas é exibido, então talvez você
não consiga ver exemplos de todos os anos.

# Exercício 2: Explorar dados em um dataframe

O objeto dataframe inclui uma ampla gama de funções que você pode usar
para filtrar, agrupar e manipular os dados que ele contém.

## Tarefa 1: Filtrar um dataframe

1.  Use o ícone **+ Code** abaixo da saída da célula para adicionar uma
    nova célula de código ao notebook e insira o seguinte código nela.

**CodeCopy**

> customers = df\['CustomerName', 'Email'\]
>
> print(customers.count())
>
> print(customers.distinct().count())
>
> display(customers.distinct())
>
> ![](./media/image39.png)

2.  **Execute** a nova célula de código e revise os resultados. Observe
    os seguintes detalhes:

    - Quando você executa uma operação em um dataframe , o resultado é
      um novo dataframe (neste caso, um novo **customers** o dataframe é
      criado selecionando um subconjunto específico de colunas do
      **df** dataframe)

    - Os dataframes fornecem funções como **count** e **distinct** que
      podem ser usadas para resumir e filtrar os dados que eles contêm.

    - A sintaxe dataframe\['Field1', 'Field2', ...\] é uma forma
      abreviada de definir um subconjunto de colunas. Você também pode
      usar o método **select**, então a primeira linha do código acima
      poderia ser escrita como customers = df.select("CustomerName",
      "Email")

> ![](./media/image40.png)

3.  Modifique o código, substitua todo o código da **cell** pelo código
    a seguir e clique no botão **▷ Run cell** da seguinte maneira:

> CodeCopy
>
> customers = df.select("CustomerName",
> "Email").where(df\['Item'\]=='Road-250 Red, 52')
>
> print(customers.count())
>
> print(customers.distinct().count())
>
> display(customers.distinct())

4.  **Execute** o código modificado para visualizar os clientes que
    compraram o ***Road-250 Red, 52* product**. Observe que você pode
    “**chain**” várias funções para que a saída de uma função se torne a
    entrada da próxima — neste caso, o dataframe criado pelo método
    **select** é o dataframe de origem para o método **where**, usado
    para aplicar os critérios de filtragem.

> ![](./media/image41.png)

## Tarefa 2: Agregar e agrupar dados em um dataframe

1.  Clique em **+ Code** e copie e cole o código abaixo e depois clique
    no botão **Run cell**.

CodeCopy

> productSales = df.select("Item", "Quantity").groupBy("Item").sum()
>
> display(productSales)
>
> ![](./media/image42.png)

2.  Observe que os resultados mostram a soma das quantidades pedidas
    agrupadas por produto. O método **groupBy** agrupa as linhas por
    *Item* e a função de agregação **Sum** subsequente é aplicada a
    todas as colunas numéricas restantes (neste caso, *Quantidade*).

![](./media/image43.png)

3.  Clique em **+ Code** e copie e cole o código abaixo e depois clique
    no botão **Run cell**.

> **CodeCopy**
>
> from pyspark.sql.functions import \*
>
> yearlySales =
> df.select(year("OrderDate").alias("Year")).groupBy("Year").count().orderBy("Year")
>
> display(yearlySales)

![](./media/image44.png)

4.  Observe que os resultados mostram o número de pedidos de venda por
    ano. Observe que o método **select** inclui uma função SQL **year**
    para extrair o componente year do campo *OrderDate* (motivo pelo
    qual o código inclui uma instrução **import** para importar funções
    da biblioteca Spark SQL). Em seguida, ele usa um método **alias**
    para atribuir um nome de coluna ao valor do ano extraído. Os dados
    são então agrupados pela coluna *Year derivada* e a contagem de
    linhas em cada grupo é calculada antes que o método **orderBy** seja
    usado para classificar o dataframe resultante .

![](./media/image45.png)

# Exercício 3: Use o Spark para transformar arquivos de dados

Uma tarefa comum para engenheiros de dados é inserir dados em um formato
ou estrutura específica e transformá-los para processamento ou análise
posterior.

## Tarefa 1: Use métodos e funções de dataframe para transformar dados

1.  Clique em + Code e copie e cole o código abaixo

**CodeCopy**

> from pyspark.sql.functions import \*
>
> \## Create Year and Month columns
>
> transformed_df = df.withColumn("Year",
> year(col("OrderDate"))).withColumn("Month", month(col("OrderDate")))
>
> \# Create the new FirstName and LastName fields
>
> transformed_df = transformed_df.withColumn("FirstName",
> split(col("CustomerName"), " ").getItem(0)).withColumn("LastName",
> split(col("CustomerName"), " ").getItem(1))
>
> \# Filter and reorder columns
>
> transformed_df = transformed_df\["SalesOrderNumber",
> "SalesOrderLineNumber", "OrderDate", "Year", "Month", "FirstName",
> "LastName", "Email", "Item", "Quantity", "UnitPrice", "Tax"\]
>
> \# Display the first five orders
>
> display(transformed_df.limit(5))

![](./media/image46.png)

2.  **Execute** o código para criar um novo dataframe a partir dos dados
    do pedido original com as seguintes transformações:

    - Adicione as colunas **Year** e **Month** com base na coluna
      **OrderDate**.

    - Adicione as colunas **FirstName** e **LastName** com base na
      coluna **CustomerName** .

    - Filtre e reordene as colunas, removendo a coluna **CustomerName**
      .

![](./media/image47.png)

3.  Revise a saída e verifique se as transformações foram feitas nos
    dados.

![](./media/image48.png)

Você pode usar todo o poder da biblioteca Spark SQL para transformar os
dados filtrando linhas, derivando, removendo, renomeando colunas e
aplicando quaisquer outras modificações de dados necessárias.

**Dica** : consulte a [*Spark dataframe
documentation*](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html)
para saber mais sobre os métodos do objeto Dataframe .

## Tarefa 2: Salvar os dados transformados

1.  **Add a new cell** com o seguinte código para salvar o dataframe
    transformado no formato Parquet (substituindo os dados se já
    existirem). **Execute** a célula e aguarde a mensagem informando que
    os dados foram salvos.

> CodeCopy
>
> transformed_df.write.mode("overwrite").parquet('Files/transformed_data/orders')
>
> print ("Transformed data saved!")
>
> **Observação** : Normalmente, o formato *Parquet* é o preferido para
> arquivos de dados que você usará para análises posteriores ou inserção
> em um repositório analítico. O Parquet é um formato muito eficiente,
> suportado pela maioria dos sistemas de análise de dados em larga
> escala . Na verdade, às vezes, sua necessidade de transformação de
> dados pode ser simplesmente converter dados de outro formato (como
> CSV) para Parquet!

![](./media/image49.png)

![](./media/image50.png)

2.  Em seguida, no painel **Lakehouse explorer** à esquerda, no menu
    **…** do nó **Files** , selecione **Refresh**.

> ![](./media/image51.png)

3.  Clique na pasta **transformed_data** para verificar se contém uma
    nova pasta chamada **orders**, que por sua vez contém um ou mais
    **Parquet files**.

![](./media/image52.png)

4.  Clique em **+ Code** seguindo o código para carregar um novo
    dataframe dos arquivos parquet na pasta **transformed_data -\>
    orders** :

> **CodeCopy**
>
> orders_df =
> spark.read.format("parquet").load("Files/transformed_data/orders")
>
> display(orders_df)
>
> ![](./media/image53.png)

5.  **Execute** a célula e verifique se os resultados mostram os dados
    do pedido que foram carregados dos arquivos parquet.

> ![](./media/image54.png)

## Tarefa 3: Salvar dados em arquivos particionados

1.  Adicione uma nova célula. Clique em **+ Code** com o seguinte
    código; que salva o dataframe , particionando os dados por **Year**
    e **Month**. **Execute** a célula e aguarde a mensagem de que os
    dados foram salvos.

> CodeCopy
>
> orders_df.write.partitionBy("Year","Month").mode("overwrite").parquet("Files/partitioned_data")
>
> print ("Transformed data saved!")
>
> ![](./media/image55.png)
>
> ![](./media/image56.png)

2.  Em seguida, no painel **Lakehouse explorer** à esquerda, no menu
    **…** do nó **Files**, selecione **Refresh.**

![](./media/image57.png)

![](./media/image58.png)

3.  Expanda a pasta **partitioned_orders** para verificar se ela contém
    uma hierarquia de pastas denominada **Year= *xxxx ***, cada uma
    contendo pastas denominadas **Month= *xxxx ***. Cada pasta de mês
    contém um arquivo parquet com os pedidos daquele mês.

![](./media/image59.png)

![](./media/image60.png)

> Particionar arquivos de dados é uma maneira comum de otimizar o
> desempenho ao lidar com grandes volumes de dados. Essa técnica pode
> melhorar significativamente o desempenho e facilitar a filtragem de
> dados.

4.  Adicione uma nova célula, clique **+ Code** com o seguinte código
    para carregar um novo dataframe do arquivo **orders.parquet**:

> CodeCopy
>
> orders_2021_df =
> spark.read.format("parquet").load("Files/partitioned_data/Year=2021/Month=\*")
>
> display(orders_2021_df)

![](./media/image61.png)

5.  **Execute** a célula e verifique se os resultados mostram os dados
    do pedido de vendas em 2021. Observe que as colunas de
    particionamento especificadas no caminho (**Year** e **Month**) não
    estão incluídas no dataframe.

![](./media/image62.png)

# **Exercício 3: Trabalhar com tabelas e SQL**

Como você viu, os métodos nativos do objeto dataframe permitem consultar
e analisar dados de um arquivo com bastante eficiência. No entanto,
muitos analistas de dados se sentem mais confortáveis trabalhando com
tabelas que podem consultar usando a sintaxe SQL. O Spark fornece um
*metastore* no qual você pode definir tabelas relacionais. A biblioteca
Spark SQL que fornece o objeto dataframe também suporta o uso de
instruções SQL para consultar tabelas no metastore. Ao usar esses
recursos do Spark, você pode combinar a flexibilidade de um data lake
com o esquema de dados estruturados e as consultas baseadas em SQL de um
data warehouse relacional – daí o termo "data lakehouse".

## Tarefa 1: Criar uma tabela gerenciada

As tabelas em um metastore do Spark são abstrações relacionais sobre
arquivos no data lake. As tabelas podem ser *gerenciadas* (nesse caso,
os arquivos são gerenciados pelo metastore) ou *externas* (nesse caso, a
tabela faz referência a um local de arquivo no data lake que você
gerencia independentemente do metastore).

1.  Adicione um novo código, clique na célula **+ Code** para o notebook
    e insira o seguinte código, que salva o dataframe de dados do pedido
    de vendas como uma tabela chamada **salesorders**:

> CodeCopy
>
> \# Create a new table
>
> df.write.format("delta").saveAsTable("salesorders")
>
> \# Get the table description
>
> spark.sql("DESCRIBE EXTENDED salesorders").show(truncate=False)

![A screenshot of a computer Description automatically
generated](./media/image63.png)

**Observação** : Vale a pena observar alguns pontos sobre este exemplo.
Primeiro, nenhum caminho explícito é fornecido, portanto, os arquivos da
tabela serão gerenciados pelo metastore. Segundo, a tabela é salva no
formato **delta** . Você pode criar tabelas com base em vários formatos
de arquivo (incluindo CSV, Parquet, Avro e outros), mas *o Delta Lake* é
uma tecnologia Spark que adiciona recursos de banco de dados relacional
às tabelas, versionamento de linhas e outros recursos úteis. Criar
tabelas no formato Delta é a opção preferida para data lakehouses no
Fabric.

2.  **Execute** a célula de código e revise a saída, que descreve a
    definição da nova tabela.

![A screenshot of a computer Description automatically
generated](./media/image64.png)

3.  No painel **Lakehouse** **explorer**, no menu **…** da pasta
    **Tables**, selecione **Refresh.**

![A screenshot of a computer Description automatically
generated](./media/image65.png)

4.  Em seguida, expanda o nó **Tables** e verifique se a tabela
    **salesorders** foi criada.

> ![A screenshot of a computer Description automatically
> generated](./media/image66.png)

5.  Passe o mouse sobre a tabela **salesorders** e clique nas
    reticências horizontais (...). Navegue e clique em **Load data** e
    selecione **Spark** .

![A screenshot of a computer Description automatically
generated](./media/image67.png)

6.  Clique no botão **▷ Run cell**, que usa a biblioteca Spark SQL para
    inserir uma consulta SQL na tabela **salesorder** no código PySpark
    e carregar os resultados da consulta em um dataframe.

> CodeCopy
>
> df = spark.sql("SELECT \* FROM \[your_lakehouse\].salesorders LIMIT
> 1000")
>
> display(df)

![A screenshot of a computer program Description automatically
generated](./media/image68.png)

![](./media/image69.png)

## Tarefa 2: Criar uma tabela externa

Você também pode criar tabelas *externas* para as quais os metadados do
esquema são definidos no metastore do lakehouse, mas os arquivos de
dados são armazenados em um local externo.

1.  Abaixo dos resultados retornados pela primeira célula de código, use
    o botão **+Code** para adicionar uma nova célula de código, caso
    ainda não exista uma. Em seguida, insira o seguinte código na nova
    célula.

CodeCopy

df.write.format("delta").saveAsTable("external_salesorder",
path="\<abfs_path\>/external_salesorder")

![A screenshot of a computer Description automatically
generated](./media/image70.png)

2.  No painel **Lakehouse explorer**, no menu **…** da pasta **Files**,
    selecione **Copy ABFS path** no bloco de notas.

> O caminho ABFS é o caminho totalmente qualificado para a pasta
> **Files** no armazenamento OneLake para seu lakehouse - semelhante a
> este:

abfss://dp_Fabric29@onelake.dfs.fabric.microsoft.com/Fabric_lakehouse.Lakehouse/Files/external_salesorder

![A screenshot of a computer Description automatically
generated](./media/image71.png)

3.  Agora, vá para a célula de código e substitua **\<abfs_path\>** pelo
    **path** que você copiou para o bloco de notas, para que o código
    salve o dataframe como uma tabela externa com arquivos de dados em
    uma pasta chamada **external_salesorder** na sua pasta **Files**. O
    caminho completo deve ser semelhante a este:

abfss://dp_Fabric29@onelake.dfs.fabric.microsoft.com/Fabric_lakehouse.Lakehouse/Files/external_salesorder

4.  Use o botão **▷ (*Run cell*)** à esquerda da célula para executá-la.

![](./media/image72.png)

5.  No painel **Lakehouse explorer**, no menu **…** da pasta **Tables**,
    selecione **Refresh**.

![A screenshot of a computer Description automatically
generated](./media/image73.png)

6.  Em seguida, expanda o nó **Tables** e verifique se a tabela
    **external_salesorder** foi criada.

![A screenshot of a computer Description automatically
generated](./media/image74.png)

7.  No painel **Lakehouse explorer**, no menu **…** da pasta **Files**,
    selecione **Refresh**.

![A screenshot of a computer Description automatically
generated](./media/image75.png)

8.  Em seguida, expanda o nó **Files** e verifique se a pasta
    **external_salesorder** foi criada para os arquivos de dados da
    tabela.

![A screenshot of a computer Description automatically
generated](./media/image76.png)

## Tarefa 3: Comparar tabelas gerenciadas e externas

Vamos explorar as diferenças entre tabelas gerenciadas e externas.

1.  Abaixo dos resultados retornados pela célula de código, use o botão
    **+ Code** para adicionar uma nova célula de código. Copie o código
    abaixo na célula de código e use o botão **▷ (*Run cell*)** à
    esquerda da célula para executá-lo.

> SqlCopy
>
> %%sql
>
> DESCRIBE FORMATTED salesorders;

![A screenshot of a computer Description automatically
generated](./media/image77.png)

![A screenshot of a computer Description automatically
generated](./media/image78.png)

2.  Nos resultados, visualize a propriedade **Location** da tabela, que
    deve ser um caminho para o armazenamento OneLake para o lakehouse
    terminando em **/Tables/salesorders** (talvez seja necessário
    ampliar a coluna **Data type** para ver o caminho completo).

![A screenshot of a computer Description automatically
generated](./media/image79.png)

3.  Modifique o comando **DESCRIBE** para mostrar os detalhes da tabela
    **external_saleorder**, conforme mostrado aqui.

4.  Abaixo dos resultados retornados pela célula de código, use o botão
    **+ Code** para adicionar uma nova célula de código. Copie o código
    abaixo e use o botão **▷ (*Run cell*)** à esquerda da célula para
    executá-lo.

> SqlCopy
>
> %%sql
>
> DESCRIBE FORMATTED external_salesorder;

![A screenshot of a email Description automatically
generated](./media/image80.png)

5.  Nos resultados, visualize a propriedade **Location** da tabela, que
    deve ser um caminho para o armazenamento OneLake para o lakehouse
    terminando em **/Files/external_saleorder** (talvez seja necessário
    ampliar a coluna **Data type** para ver o caminho completo).

![A screenshot of a computer Description automatically
generated](./media/image81.png)

## Tarefa 4: Executar código SQL em uma célula

Embora seja útil poder inserir instruções SQL em uma célula que contém
código PySpark, os analistas de dados geralmente querem trabalhar
diretamente em SQL.

1.  Clique em **+ Code** para a célula do notebook e insira o seguinte
    código. Clique no botão **▷ Run cell** e revise os resultados.
    Observe que:

    - A linha %%sql no início da célula (chamada de *magic*) indica que
      o tempo de execução da linguagem Spark SQL deve ser usado para
      executar o código nesta célula em vez do PySpark.

    - O código SQL faz referência à tabela **salesorders** que você
      criou anteriormente.

    - A saída da consulta SQL é exibida automaticamente como resultado
      na célula

> SqlCopy
>
> %%sql
>
> SELECT YEAR(OrderDate) AS OrderYear,
>
> SUM((UnitPrice \* Quantity) + Tax) AS GrossRevenue
>
> FROM salesorders
>
> GROUP BY YEAR(OrderDate)
>
> ORDER BY OrderYear;

![A screenshot of a computer Description automatically
generated](./media/image82.png)

![](./media/image83.png)

**Observação** : para obter mais informações sobre Spark SQL e
dataframes, consulte a [*Spark SQL
documentation*](https://spark.apache.org/docs/2.2.0/sql-programming-guide.html).

# Exercício 4: Visualizar dados com Spark

Uma imagem vale mais que mil palavras, e um gráfico costuma ser melhor
do que mil linhas de dados. Embora os notebooks no Fabric incluam uma
visualização de gráfico integrada para dados exibidos a partir de um
dataframe ou consulta Spark SQL, não foram projetados para gráficos
abrangentes. No entanto, você pode usar bibliotecas gráficas Python,
como **matplotlib** e **seaborn**, para criar gráficos a partir de dados
em dataframes.

## Tarefa 1: Visualizar os resultados como um gráfico

1.  Clique em **+ Code** para a célula do notebook e insira o seguinte
    código. Clique no botão **▷ Run cell** e observe que ele retorna os
    dados da visualização **salesorders** que você criou anteriormente.

> SqlCopy
>
> %%sql
>
> SELECT \* FROM salesorders

![A screenshot of a computer Description automatically
generated](./media/image84.png)

![A screenshot of a computer Description automatically
generated](./media/image85.png)

2.  Na seção de resultados abaixo da célula, altere a opção **View** da
    **Table** para **Chart**.

![A screenshot of a computer Description automatically
generated](./media/image86.png)

3.  Use o botão **View options** no canto superior direito do gráfico
    para exibir o painel de opções do gráfico. Em seguida, defina as
    opções a seguir e selecione **Apply**:

    - **Chart type**: Bar chart

    - **Key**: Item

    - **Values**: Quantity

    - **Series Group**: *deixar em branco*

    - **Aggregation**: Sum

    - **Stacked**: *Não selecionado*

![A blue barcode on a white background Description automatically
generated](./media/image87.png)

![A screenshot of a graph Description automatically
generated](./media/image88.png)

4.  Verifique se o gráfico é semelhante a este

![A screenshot of a computer Description automatically
generated](./media/image89.png)

## Tarefa 2: Comece a usar o matplotlib

1.  Clique em **+ Code** e copie e cole o código abaixo. **Execute** o
    código e observe que ele retorna um dataframe do Spark contendo a
    receita anual.

> CodeCopy
>
> sqlQuery = "SELECT CAST(YEAR(OrderDate) AS CHAR(4)) AS OrderYear, \\
>
> SUM((UnitPrice \* Quantity) + Tax) AS GrossRevenue \\
>
> FROM salesorders \\
>
> GROUP BY CAST(YEAR(OrderDate) AS CHAR(4)) \\
>
> ORDER BY OrderYear"
>
> df_spark = spark.sql(sqlQuery)
>
> df_spark.show()

![A screenshot of a computer Description automatically
generated](./media/image90.png)

![](./media/image91.png)

2.  Para visualizar os dados como um gráfico, começaremos usando a
    biblioteca **matplotlib** Python. Esta biblioteca é a principal
    biblioteca de plotagem na qual muitas outras se baseiam e oferece
    bastante flexibilidade na criação de gráficos.

3.  Clique em **+ Code** e copie e cole o código abaixo.

**CodeCopy**

> from matplotlib import pyplot as plt
>
> \# matplotlib requires a Pandas dataframe, not a Spark one
>
> df_sales = df_spark.toPandas()
>
> \# Create a bar plot of revenue by year
>
> plt.bar(x=df_sales\['OrderYear'\], height=df_sales\['GrossRevenue'\])
>
> \# Display the plot
>
> plt.show()

![A screenshot of a computer Description automatically
generated](./media/image92.png)

4.  Clique no botão **Run cell** e revise os resultados, que consistem
    em um gráfico de colunas com a receita bruta total de cada ano.
    Observe as seguintes características do código usado para gerar este
    gráfico:

    - A biblioteca **matplotlib** requer um dataframe *Pandas*, então
      você precisa converter o dataframe *Spark* retornado pela consulta
      Spark SQL para este formato.

    - No centro da biblioteca **matplotlib** está o objeto **pyplot**.
      Esta é a base para a maioria das funcionalidades de plotagem.

    - As configurações padrão resultam em um gráfico utilizável, mas há
      um grande espaço para personalizá-lo.

![A screenshot of a computer screen Description automatically
generated](./media/image93.png)

5.  Modifique o código para plotar o gráfico da seguinte forma,
    substitua todo o código na **cell** pelo seguinte código e clique no
    botão **▷ Run cell** e revise a saída

> CodeCopy
>
> from matplotlib import pyplot as plt
>
> \# Clear the plot area
>
> plt.clf()
>
> \# Create a bar plot of revenue by year
>
> plt.bar(x=df_sales\['OrderYear'\], height=df_sales\['GrossRevenue'\],
> color='orange')
>
> \# Customize the chart
>
> plt.title('Revenue by Year')
>
> plt.xlabel('Year')
>
> plt.ylabel('Revenue')
>
> plt.grid(color='#95a5a6', linestyle='--', linewidth=2, axis='y',
> alpha=0.7)
>
> plt.xticks(rotation=45)
>
> \# Show the figure
>
> plt.show()

![A screenshot of a graph Description automatically
generated](./media/image94.png)

6.  O gráfico agora inclui um pouco mais de informações. Tecnicamente,
    um gráfico está contido em uma **Figure**. Nos exemplos anteriores,
    a figura foi criada implicitamente para você; mas você pode criá-la
    explicitamente.

7.  Modifique o código para plotar o gráfico da seguinte maneira:
    substitua todo o código na **cell** pelo código a seguir.

> CodeCopy
>
> from matplotlib import pyplot as plt
>
> \# Clear the plot area
>
> plt.clf()
>
> \# Create a Figure
>
> fig = plt.figure(figsize=(8,3))
>
> \# Create a bar plot of revenue by year
>
> plt.bar(x=df_sales\['OrderYear'\], height=df_sales\['GrossRevenue'\],
> color='orange')
>
> \# Customize the chart
>
> plt.title('Revenue by Year')
>
> plt.xlabel('Year')
>
> plt.ylabel('Revenue')
>
> plt.grid(color='#95a5a6', linestyle='--', linewidth=2, axis='y',
> alpha=0.7)
>
> plt.xticks(rotation=45)
>
> \# Show the figure
>
> plt.show()

![A screenshot of a computer program Description automatically
generated](./media/image95.png)

8.  **Execute novamente** a célula de código e visualize os resultados.
    A figura determina o formato e o tamanho do gráfico.

> Uma figura pode conter vários subgráficos, cada um em seu próprio
> *eixo* .

![A screenshot of a computer Description automatically
generated](./media/image96.png)

9.  Modifique o código para plotar o gráfico da seguinte forma.
    **Execute novamente** a célula de código e visualize os resultados.
    A figura contém os subplots especificados no código.

> CodeCopy
>
> from matplotlib import pyplot as plt
>
> \# Clear the plot area
>
> plt.clf()
>
> \# Create a figure for 2 subplots (1 row, 2 columns)
>
> fig, ax = plt.subplots(1, 2, figsize = (10,4))
>
> \# Create a bar plot of revenue by year on the first axis
>
> ax\[0\].bar(x=df_sales\['OrderYear'\],
> height=df_sales\['GrossRevenue'\], color='orange')
>
> ax\[0\].set_title('Revenue by Year')
>
> \# Create a pie chart of yearly order counts on the second axis
>
> yearly_counts = df_sales\['OrderYear'\].value_counts()
>
> ax\[1\].pie(yearly_counts)
>
> ax\[1\].set_title('Orders per Year')
>
> ax\[1\].legend(yearly_counts.keys().tolist())
>
> \# Add a title to the Figure
>
> fig.suptitle('Sales Data')
>
> \# Show the figure
>
> plt.show()

![A screenshot of a computer program Description automatically
generated](./media/image97.png)

![](./media/image98.png)

**Observação:** Para saber mais sobre plotagem com matplotlib, consulte
a [*matplotlib documentation*](https://matplotlib.org/).

## Tarefa 3: Use a biblioteca seaborn

Embora o **matplotlib** permita criar gráficos complexos de vários
tipos, ele pode exigir um código complexo para obter os melhores
resultados. Por esse motivo, ao longo dos anos, muitas bibliotecas novas
foram criadas com base no matplotlib para abstrair sua complexidade e
aprimorar seus recursos. Uma dessas bibliotecas é a **Seaborn** .

1.  Clique em **+ Code** e copie e cole o código abaixo.

CodeCopy

> import seaborn as sns
>
> \# Clear the plot area
>
> plt.clf()
>
> \# Create a bar chart
>
> ax = sns.barplot(x="OrderYear", y="GrossRevenue", data=df_sales)
>
> plt.show()

![A screenshot of a graph Description automatically
generated](./media/image99.png)

2.  **Execute** o código e observe que ele exibe um gráfico de barras
    usando a biblioteca seaborn.

![A screenshot of a graph Description automatically
generated](./media/image100.png)

3.  **Modifique** o código da seguinte forma. **Execute** o código
    modificado e observe que o Seaborn permite que você defina um tema
    de cores consistente para seus gráficos.

> CodeCopy
>
> import seaborn as sns
>
> \# Clear the plot area
>
> plt.clf()
>
> \# Set the visual theme for seaborn
>
> sns.set_theme(style="whitegrid")
>
> \# Create a bar chart
>
> ax = sns.barplot(x="OrderYear", y="GrossRevenue", data=df_sales)
>
> plt.show()
>
> ![](./media/image101.png)

4.  **Modifique** o código novamente da seguinte forma. **Execute** o
    código modificado para visualizar a receita anual como um gráfico de
    linhas.

> CodeCopy
>
> import seaborn as sns
>
> \# Clear the plot area
>
> plt.clf()
>
> \# Create a bar chart
>
> ax = sns.lineplot(x="OrderYear", y="GrossRevenue", data=df_sales)
>
> plt.show()

![](./media/image102.png)

**Observação:** Para saber mais sobre plotagem com o Seaborn, consulte a
[*seaborn documentation*](https://seaborn.pydata.org/index.html).

## Tarefa 4: Usar tabelas delta para streaming de dados

O Delta Lake suporta streaming de dados. As tabelas Delta podem ser um
*coletor* ou uma *fonte* para fluxos de dados criados usando a API Spark
Structured Streaming. Neste exemplo, você usará uma tabela Delta como
coletor para alguns dados de streaming em um cenário simulado de
internet das coisas (IoT).

1.  Clique em **+ Code** e copie e cole o código abaixo e depois clique
    no botão **Run cell**.

CodeCopy

> from notebookutils import mssparkutils
>
> from pyspark.sql.types import \*
>
> from pyspark.sql.functions import \*
>
> \# Create a folder
>
> inputPath = 'Files/data/'
>
> mssparkutils.fs.mkdirs(inputPath)
>
> \# Create a stream that reads data from the folder, using a JSON
> schema
>
> jsonSchema = StructType(\[
>
> StructField("device", StringType(), False),
>
> StructField("status", StringType(), False)
>
> \])
>
> iotstream =
> spark.readStream.schema(jsonSchema).option("maxFilesPerTrigger",
> 1).json(inputPath)
>
> \# Write some event data to the folder
>
> device_data = '''{"device":"Dev1","status":"ok"}
>
> {"device":"Dev1","status":"ok"}
>
> {"device":"Dev1","status":"ok"}
>
> {"device":"Dev2","status":"error"}
>
> {"device":"Dev1","status":"ok"}
>
> {"device":"Dev1","status":"error"}
>
> {"device":"Dev2","status":"ok"}
>
> {"device":"Dev2","status":"error"}
>
> {"device":"Dev1","status":"ok"}'''
>
> mssparkutils.fs.put(inputPath + "data.txt", device_data, True)
>
> print("Source stream created...")

![A screenshot of a computer program Description automatically
generated](./media/image103.png)

![A screenshot of a computer Description automatically
generated](./media/image104.png)

2.  Certifique-se de que a mensagem ***Source stream created…*** seja
    impressa. O código que você acabou de executar criou uma fonte de
    dados de streaming com base em uma pasta na qual alguns dados foram
    salvos, representando leituras de dispositivos IoT hipotéticos.

3.  Clique em **+ Code** e copie e cole o código abaixo e depois clique
    no botão **Run cell**.

CodeCopy

> \# Write the stream to a delta table
>
> delta_stream_table_path = 'Tables/iotdevicedata'
>
> checkpointpath = 'Files/delta/checkpoint'
>
> deltastream =
> iotstream.writeStream.format("delta").option("checkpointLocation",
> checkpointpath).start(delta_stream_table_path)
>
> print("Streaming to delta sink...")

![A screenshot of a computer Description automatically
generated](./media/image105.png)

4.  Este código escreve os dados do dispositivo de streaming em formato
    delta em uma pasta chamada **iotdevicedata**. O caminho para o local
    da pasta está na pasta **Tables**, uma tabela será criada
    automaticamente para isso. Clique nas reticências horizontais ao
    lado da tabela e, em seguida, clique em **Refresh**.

![](./media/image106.png)

![A screenshot of a computer Description automatically
generated](./media/image107.png)

5.  Clique em **+ Code** e copie e cole o código abaixo e depois clique
    no botão **Run cell**.

> SqlCopy
>
> %%sql
>
> SELECT \* FROM IotDeviceData;

![A screenshot of a computer Description automatically
generated](./media/image108.png)

6.  Este código consulta a tabela **IotDeviceData**, que contém os dados
    do dispositivo da fonte de streaming.

7.  Clique em **+ Code** e copie e cole o código abaixo e depois clique
    no botão **Run cell**.

> CodeCopy
>
> \# Add more data to the source stream
>
> more_data = '''{"device":"Dev1","status":"ok"}
>
> {"device":"Dev1","status":"ok"}
>
> {"device":"Dev1","status":"ok"}
>
> {"device":"Dev1","status":"ok"}
>
> {"device":"Dev1","status":"error"}
>
> {"device":"Dev2","status":"error"}
>
> {"device":"Dev1","status":"ok"}'''
>
> mssparkutils.fs.put(inputPath + "more-data.txt", more_data, True)

![A screenshot of a computer Description automatically
generated](./media/image109.png)

8.  Este código escreve mais dados hipotéticos do dispositivo na fonte
    de streaming.

9.  Clique em **+ Code** e copie e cole o código abaixo e depois clique
    no botão **Run cell**.

> SqlCopy
>
> %%sql
>
> SELECT \* FROM IotDeviceData;
>
> ![A screenshot of a computer Description automatically
> generated](./media/image110.png)

10. Este código consulta a tabela **IotDeviceData** novamente, que agora
    deve incluir os dados adicionais que foram adicionados à fonte de
    streaming.

11. Clique em **+ Code** e copie e cole o código abaixo e depois clique
    no botão **Run cell**.

> CodeCopy
>
> deltastream.stop()

![A screenshot of a computer Description automatically
generated](./media/image111.png)

12. Este código interrompe o fluxo.

## Tarefa 5: Salve o notebook e encerre a sessão do Spark

Agora que você terminou de trabalhar com os dados, pode salvar o
notebook com um nome significativo e encerrar a sessão do Spark.

1.  Na barra de menu do notebook, use o Ícone ⚙️ **Settings** para
    visualizar as configurações do notebook.

![A screenshot of a computer Description automatically
generated](./media/image112.png)

2.  Defina o **Name** do notebook como ++**Explore Sales Orders++** e
    feche o painel de configurações.

![A screenshot of a computer Description automatically
generated](./media/image113.png)

3.  No menu do notebook, selecione **Stop session** para encerrar a
    sessão do Spark.

![A screenshot of a computer Description automatically
generated](./media/image114.png)

![A screenshot of a computer Description automatically
generated](./media/image115.png)

# Exercício 5: Criar um fluxo de dados (Gen2) no Microsoft Fabric

No Microsoft Fabric, os Fluxos de Dados (Gen2) se conectam a várias
fontes de dados e realizam transformações no Power Query Online. Eles
podem ser usados em Pipelines de Dados para inserir dados em um
lakehouse ou outro armazenamento analítico, ou para definir um conjunto
de dados para um relatório do Power BI.

Este exercício foi desenvolvido para apresentar os diferentes elementos
dos Dataflows (Gen2), e não para criar uma solução complexa que possa
existir em uma empresa.

## Tarefa 1: Criar um fluxo de dados (Gen2) para inserir dados

Agora que você tem um lakehouse, precisa inserir alguns dados nele. Uma
maneira de fazer isso é definir um fluxo de dados que encapsule um
processo de *extração, transformação e carregamento* (ETL).

1.  Agora, clique em **Fabric_lakehouse** no painel de navegação do lado
    esquerdo.

![A screenshot of a computer Description automatically
generated](./media/image116.png)

2.  Na página inicial do **Fabric_lakehouse**, clique na seta suspensa
    em **Get data** e selecione **New Dataflow Gen2.** O editor do Power
    Query para o seu novo fluxo de dados será aberto.

![](./media/image117.png)

3.  No painel do **Power Query** , na **aba Home**, clique em **Import
    from a Text/CSV file**.

![](./media/image118.png)

4.  No painel **Connect to data source**, em **Connection settings**,
    selecione o botão **Link to file (Preview)**

- **Link to file**: *Selected*

- **File path or
  URL**: <https://raw.githubusercontent.com/MicrosoftLearning/dp-data/main/orders.csv>

![](./media/image119.png)

5.  No painel **Connect to data source**, em **Connection credentials,**
    insira os seguintes detalhes e clique no botão **Next**.

    - **Connection**: Create new connection

    - **data gateway**: (none)

    - **Authentication kind**: Organizational account

![](./media/image120.png)

6.  No painel **Preview file data,** clique em **Create** para criar a
    fonte de dados.![A screenshot of a computer Description
    automatically generated](./media/image121.png)

7.  O editor do **Power Query** mostra a fonte de dados e um conjunto
    inicial de etapas de consulta para formatar os dados.

![A screenshot of a computer Description automatically
generated](./media/image122.png)

8.  Na barra de ferramentas, selecione a aba **Add column**. Em seguida,
    selecione **Custom column.**

![A screenshot of a computer Description automatically
generated](./media/image123.png) 

9.  Defina o nome da nova coluna como **MonthNo**, defina o tipo de dado
    como **Whole Number** e adicione a seguinte fórmula:
    **Date.Month(\[OrderDate\])** em **Custom column formula**.
    Selecione **OK** .

![A screenshot of a computer Description automatically
generated](./media/image124.png)

10. Observe como a etapa para adicionar a coluna personalizada é
    adicionada à consulta. A coluna resultante é exibida no painel de
    dados.

![A screenshot of a computer Description automatically
generated](./media/image125.png)

**Dica:** No painel Configurações da Consulta, à direita, observe que as
**Applied Steps** incluem cada etapa da transformação. Na parte
inferior, você também pode alternar o botão **Diagram flow** para ativar
o Diagrama Visual das etapas.

As etapas podem ser movidas para cima ou para baixo, editadas
selecionando o ícone de engrenagem, e você pode selecionar cada etapa
para ver as transformações aplicadas no painel de visualização.

Tarefa 2: Adicionar destino de dados para o Dataflow

1.  Na barra de ferramentas do **Power Query**, selecione a aba
    **Home**. Em seguida, no menu suspenso **Data destination,**
    selecione **Lakehouse** (se ainda não estiver selecionado).

![](./media/image126.png)

![A screenshot of a computer Description automatically
generated](./media/image127.png)

**Observação:** se esta opção estiver desativada, talvez você já tenha
um destino de dados definido. Verifique o destino de dados na parte
inferior do painel Configurações da consulta, no lado direito do editor
do Power Query. Se um destino já estiver definido, você poderá alterá-lo
usando a engrenagem.

2.  Clique no ícone **Settings** ao lado da opção **Lakehouse**
    selecionada.

![A screenshot of a computer Description automatically
generated](./media/image128.png)

3.  Na caixa de diálogo **Connect to data destination**, selecione
    **Edit connection.**

![A screenshot of a computer Description automatically
generated](./media/image129.png)

4.  Na caixa de diálogo **Connect to data destination**, selecione
    **sign in** usando sua conta organizacional do Power BI para definir
    a identidade que o fluxo de dados usa para acessar o lakehouse.

![A screenshot of a computer Description automatically
generated](./media/image130.png)

![A screenshot of a computer Description automatically
generated](./media/image131.png)

5.  Na caixa de diálogo Connect to data destination, selecione **Next**

![A screenshot of a computer Description automatically
generated](./media/image132.png)

6.  Na caixa de diálogo Connect to data destination, selecione **New
    table**. Clique na **pasta Lakehouse** , selecione seu workspace –
    **dp_FabricXX** e então selecione seu lakehouse, ou seja,
    **Fabric_lakehouse.** Em seguida, especifique o nome da tabela como
    **orders** e selecione o botão **Next** .

![A screenshot of a computer Description automatically
generated](./media/image133.png)

7.  Na caixa de diálogo **Choose destination settings**, em **Use
    automatic settings off** e o **Update method,** selecione **Append**
    e clique no botão **Save settings**.

![](./media/image134.png)

8.  O destino **Lakehouse** é indicado como um **icon** na **query** no
    editor do Power Query.

![A screenshot of a computer Description automatically
generated](./media/image135.png)

![A screenshot of a computer Description automatically
generated](./media/image136.png)

9.  Selecione **Publish** para publicar o fluxo de dados. Em seguida,
    aguarde a criação do fluxo de dados do **Dataflow 1** no seu
    workspace**.**

![A screenshot of a computer Description automatically
generated](./media/image137.png)

10. Após a publicação, você pode clicar com o botão direito no fluxo de
    dados do seu workspace, selecionar **Properties** e renomear seu
    fluxo de dados.

![A screenshot of a computer Description automatically
generated](./media/image138.png)

11. Na caixa de diálogo **Dataflow1**, insira o **Name** como
    **Gen2_Dataflow** e clique no botão **Save**.

![A screenshot of a computer Description automatically
generated](./media/image139.png)

![](./media/image140.png)

## Tarefa 3: Adicionar um fluxo de dados a um pipeline

Você pode incluir um fluxo de dados como uma atividade em um pipeline.
Pipelines são usados para orquestrar atividades de inserção e
processamento de dados, permitindo combinar fluxos de dados com outros
tipos de operação em um único processo agendado. Pipelines podem ser
criados em algumas experiências diferentes, incluindo a experiência do
Data Factory.

1.  Na página inicial do Synapse Data Engineering, no painel
    **dp_FabricXX**, selecione **+New item** -\> **Data pipeline**

![](./media/image141.png)

2.  Na caixa de diálogo **New pipeline**, insira **Load data** no campo
    **Name** e clique no botão **Create** para abrir o novo pipeline.

![A screenshot of a computer Description automatically
generated](./media/image142.png)

3.  O editor de pipeline é aberto.

![A screenshot of a computer Description automatically
generated](./media/image143.png)

> **Dica** : Se o assistente Copiar Dados abrir automaticamente,
> feche-o!

4.  Selecione **Pipeline activity** e adicione uma atividade
    **Dataflow** ao pipeline.

![](./media/image144.png)

5.  Com a nova atividade **Dataflow1** selecionada, na aba **Settings**,
    na lista suspensa **Dataflow ,** selecione **Gen2_Dataflow** (o
    fluxo de dados que você criou anteriormente)

![](./media/image145.png)

6.  Na aba **Home**, salve o pipeline usando o ícone **🖫 (*Save*)** .

![A screenshot of a computer Description automatically
generated](./media/image146.png)

7.  Use o botão **▷ Run** para executar o pipeline e aguarde a
    conclusão. Pode levar alguns minutos.

> ![A screenshot of a computer Description automatically
> generated](./media/image147.png)
>
> ![A screenshot of a computer Description automatically
> generated](./media/image148.png)

![A screenshot of a computer Description automatically
generated](./media/image149.png)

8.  Na barra de menus na borda esquerda, selecione seu workspace, ou
    seja, **dp_FabricXX** .

![A screenshot of a computer Description automatically
generated](./media/image150.png)

9.  No painel **Fabric_lakehouse** , selecione o
    **Gen2_FabricLakehouse** do tipo Lakehouse.

![A screenshot of a computer Description automatically
generated](./media/image151.png)

10. No painel **Explorer,** selecione o menu **…** para **Tables** e
    selecione **refresh**. Em seguida, expanda **Tables** e selecione a
    tabela **orders** criada pelo seu fluxo de dados.

![A screenshot of a computer Description automatically
generated](./media/image152.png)

![](./media/image153.png)

**Dica** : use o *conector de fluxos de dados do Power BI Desktop* para
se conectar diretamente às transformações de dados feitas com seu fluxo
de dados.

Você também pode fazer transformações adicionais, publicar como um novo
conjunto de dados e distribuir para o público-alvo de conjuntos de dados
especializados.

## Tarefa 4: Limpar recursos

Neste exercício, você aprendeu a usar o Spark para trabalhar com dados
no Microsoft Fabric.

Quando você terminar de explorar seu lakehouse, poderá excluir o
workspace que criou para este exercício.

1.  Na barra à esquerda, selecione o ícone do seu workspace para
    visualizar todos os itens que ele contém.

> ![A screenshot of a computer Description automatically
> generated](./media/image154.png)

2.  No menu **…** na barra de ferramentas, selecione **Workspace
    settings**.

![](./media/image155.png)

3.  Selecione **General** e clique em **Remove this workspace.**

![A screenshot of a computer settings Description automatically
generated](./media/image156.png)

4.  Na caixa de diálogo **Delete workspace?,** clique no botão
    **Delete**.

> ![A screenshot of a computer Description automatically
> generated](./media/image157.png)
>
> ![A screenshot of a computer Description automatically
> generated](./media/image158.png)

**Resumo**

Este caso de uso orienta no processo de trabalho com o Microsoft Fabric
no Power BI. Abrangendo diversas tarefas, incluindo a configuração de um
workspace, a criação de um lakehouse, o upload e o gerenciamento de
arquivos de dados e o uso de notebooks para exploração de dados. Os
participantes aprenderão a manipular e transformar dados usando o
PySpark, criar visualizações e salvar e particionar dados para consultas
eficientes.

Neste caso de uso , os participantes realizarão uma série de tarefas
focadas no trabalho com tabelas delta no Microsoft Fabric. As tarefas
abrangem o upload e a exploração de dados, a criação de tabelas delta
gerenciadas e externas, a comparação de suas propriedades. O laboratório
apresenta recursos de SQL para o gerenciamento de dados estruturados e
fornece insights sobre visualização de dados usando bibliotecas Python
como matplotlib e seaborn. Os exercícios visam fornecer uma compreensão
abrangente da utilização do Microsoft Fabric para análise de dados e da
incorporação de tabelas delta para streaming de dados em um contexto de
IoT.

Este caso de uso orienta você no processo de configuração de um
workspace do Fabric, criação de um data lakehouse e inserção de dados
para análise. Ele demonstra como definir um fluxo de dados para lidar
com operações de ETL e configurar destinos de dados para armazenar os
dados transformados. Além disso, você aprenderá como integrar o fluxo de
dados a um pipeline para processamento automatizado. Por fim, você
receberá instruções para limpar os recursos após a conclusão do
exercício.

Este laboratório fornece a você habilidades essenciais para trabalhar
com o Fabric, permitindo que você crie e gerencie workspaces, estabeleça
data lakehouses e execute transformações de dados com eficiência. Ao
incorporar fluxos de dados em pipelines, você aprenderá a automatizar
tarefas de processamento de dados, otimizando seu fluxo de trabalho e
aumentando a produtividade em cenários reais. As instruções de limpeza
garantem que você não deixe recursos desnecessários, promovendo uma
abordagem organizada e eficiente de gerenciamento de workspaces.
