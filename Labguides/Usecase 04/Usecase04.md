# ユースケース 04: Azure Databricks と Microsoft Fabri による最新のクラウド スケール分析

**紹介**

このラボでは、Azure Databricks と Microsoft Fabric
の統合について学習し、Medallion
アーキテクチャを使用してLakehouseを作成・管理する方法、Azure Databricks
を使用して Azure Data Lake Storage (ADLS) Gen2 アカウントを活用した
Delta テーブルを作成する方法、そして Azure Databricks
でデータを取り込む方法について説明します。このハンズオンガイドでは、Lakehouseの作成、データの読み込み、そして効率的なデータ分析とレポート作成を可能にする構造化データレイヤーの活用に必要な手順を順を追って説明します。

メダリオンアーキテクチャは3つの異なる層（またはゾーン）で構成されています.

- ブロンズ:
  RAWゾーンとも呼ばれるこの最初のレイヤーは、ソースデータを元の形式で保存します。このレイヤーのデータは、通常、追加のみ可能で不変です。

- シルバー：エンリッチドゾーンとも呼ばれるこのレイヤーは、ブロンズレイヤーから取得したデータを格納します。生データはクレンジングと標準化が行われ、表（行と列）として構造化されています。また、他のデータと統合することで、顧客、製品など、あらゆるビジネスエンティティのエンタープライズビューを提供することもできます。

- ゴールド：キュレーションゾーンとも呼ばれるこの最終層は、シルバー層から取得したデータを格納します。データは、下流のビジネス要件や分析要件に合わせて調整されます。テーブルは通常、スタースキーマ設計に準拠しており、パフォーマンスとユーザビリティが最適化されたデータモデルの開発をサポートします。

**目的**:

- Microsoft Fabric Lakehouse における Medallion
  アーキテクチャの原則を理解します。

- Medallion レイヤー (Bronze、Silver、Gold)
  を使用して、構造化データ管理プロセスを実装します。

- 生データを検証済みおよびエンリッチされたデータに変換し、高度な分析とレポート作成を実現します。

- データセキュリティ、CI/CD、効率的なデータクエリのベストプラクティスを学習します。

- OneLake ファイルエクスプローラーを使用して、OneLake
  にデータをアップロードします。

- Fabric ノートブックを使用して、OneLake 上のデータを読み取り、Delta
  テーブルとして書き戻します。

- Fabric ノートブックを使用して、Spark でデータを分析および変換します。

- SQL を使用して、OneLake 上のデータの 1 つのコピーをクエリします。

- Azure Databricks を使用して、Azure Data Lake Storage (ADLS) Gen2
  アカウントに Delta テーブルを作成します。

- ADLS 内の Delta テーブルへの OneLake ショートカットを作成します。

- Power BI を使用して、ADLS ショートカット経由でデータを分析します。

- Azure Databricks を使用して、OneLake 内の Delta
  テーブルを読み取って変更します。

# 手順 1: Bringing your sample data to Lakehouse

In this 手順, you'll go through the process of creating a lakehouse and
loading data into it using Microsoft Fabric.

Task:trail

## **タスク 1: Fabricワークスペースの作成**

このタスクでは、Fabric
ワークスペースを作成します。ワークスペースには、このLakehouse
チュートリアルに必要なすべての項目 (Lakehouse、データフロー、Data
Factory パイプライン、ノートブック、Power BI データセット、レポートなど)
が含まれています

1.  ブラウザを開き、アドレスバーに移動して、次のURLを入力または貼り付けて
    [https://app.fabric.microsoft.com/](https://app.fabric.microsoft.com/,)
    から**Enterボタンをクリックする。**

> ![A search engine window with a red box Description automatically
> generated with medium confidence](./media/image1.png)

2.  **Power BIウィンドに戻ります。**Power BIホームページの左側
    ナビゲーションメニューで**Workspacesに移動し、クリックする。**

![](./media/image2.png)

3.  In the Workspacesペーンに**+** **New
    workspace**ボタンをクリックする**。**

> ![](./media/image3.png)

4.  右側に表示する**Create a
    workspaceペーンで次の詳細を入力してApplyボタンをクリックする。**

[TABLE]

> ![](./media/image4.png)

![A screenshot of a computer Description automatically
generated](./media/image5.png)

5.  展開が完了するまでに待ちます。完了するには2-3分かかります。

![](./media/image6.png)

## **タスク 2: Lakehouseを作成**

1.  In the **Power BI Fabric Lakehouse
    Tutorial-XXページに左下にあるPower BIアイコンをクリックして、Data
    Engineering**を選択する。

> ![](./media/image7.png)

2.  **Synapse** **Data
    Engineering** **HomeページにLakehouseを選択して作成する。** 

![](./media/image8.png)

![A screenshot of a computer Description automatically
generated](./media/image9.png)

3.  **New
    lakehouseダイアログボックスにNameフィールドにwwilakehouse**を入力して、**Createボタンをクリックすることで新しい**lakehouseが開く。

> **注意: wwilakehouseの前にスペースを削除するよう確認する**
>
> ![](./media/image10.png)
>
> ![A screenshot of a computer Description automatically
> generated](./media/image11.png)
>
> ![](./media/image12.png)

4.  **Successfully created SQL endpoint**.を述べる通知を表示されます。

> ![](./media/image13.png)

# 手順 2: Azure Databricksを使用するMedallion Architectureを実地

# タスク 1: ブラウズレーヤの設定

1.  In the **wwilakehouseページにファイルの横にある**More
    アイコン(…)next to the files (…), and select **New subfolder**

![](./media/image14.png)

2.  On the pop-uでフォルダ名に**bronzeを入力して、**Createを選択する。

![A screenshot of a computer Description automatically
generated](./media/image15.png)

3.  それでは、ブロンズファイル(…)の横のMoreアイコンを選択してから**Upload**
    と次に**upload files**を選択する。

![A screenshot of a computer Description automatically
generated](./media/image16.png)

4.  **Upload fileペーン上でUpload fileラジオボタンを選択する。Browse
    buttonをクリックして、C:\LabFiles**に移動し、必要なsales data files
    (2019, 2020, 2021) を選択して**Open**ボタンをクリックする**。
    それから、Upload**を選択し、Lakehouseにあるファイルを新しいブロンズフォルダにアップロードする。

![A screenshot of a computer Description automatically
generated](./media/image17.png)

> ![A screenshot of a computer Description automatically
> generated](./media/image18.png)

5.  Click on
    **bronzeフォルダをクリックして、ファイルが通常にアップロードされたとファイルが反省されていることを検証する**

![A screenshot of a computer Description automatically
generated](./media/image19.png)

# 手順 3: Apache Spark でデータを変換し、メダリオン アーキテクチャで SQL でクエリを実行する

## **タスク 1: データを変換して、シルバーデルタにロードする。**

**wwilakehouse**ページにコマンドバー**にOpen
notebook**ドロップに移動してクリックして、**New notebook** を選択する。

![A screenshot of a computer Description automatically
generated](./media/image20.png)

1.  Select the first cell (which is currently a *code* cell), and then
    in the dynamic tool bar at its top-right, use the **M↓** button to
    **convert the cell to a markdown cell**.

![A screenshot of a computer Description automatically
generated](./media/image21.png)

2.  セルがマークダウン
    セルに変更されると、そこに含まれるテキストがレンダリングされます。

![A screenshot of a computer Description automatically
generated](./media/image22.png)

3.  セールを**🖉** (編集)ボタンを使用し、セルをエディットモードに切り替え、全てのテキストを置き換えてからマークダウンを次の用に変更します。

CodeCopy

\# Sales order data exploration

ノートブックにあるコードを使用し、セールスオーダーデータを探索する。

![A screenshot of a computer Description automatically
generated](./media/image23.png)

![A screenshot of a computer Description automatically
generated](./media/image24.png)

4.  ノートブックのセルの外側の任意の場所をクリックして編集を停止し、レンダリングされたマークダウンを表示します。

![A screenshot of a computer Description automatically
generated](./media/image25.png)

5.  セルアウトプットの下にある +
    Codeアイコンを使用し、ノートブックに新しいセルを追加します。

![A screenshot of a computer Description automatically
generated](./media/image26.png)

6.  今、ノートブックを使用し、ブロンズレーヤのデータをSpark
    DataFrameにロードします。
    ノートブック内の既存のセルを選択します。そこには、いくつかの簡単なコメントアウトされたコードが含まれています。
    これら 2 行を強調表示して削除します。このコードは必要ありません。

*注:
ノートブックを使用すると、Python、Scala、SQLなど、さまざまな言語でコードを実行できます。この手順では、PySparkとSQLを使用します。また、マークダウンセルを追加して、フォーマットされたテキストや画像を表示し、コードを記述することもできます。*

以下のコードを入力して「Run」をクリックしてください*。*

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

***注:
このノートブックでSparkコードを実行するのは初めてなので、Sparkセッションを開始する必要があります。そのため、最初の実行には1分ほどかかる場合があります。次回の実行はより短時間で完了します。***

![A screenshot of a computer Description automatically
generated](./media/image28.png)

7.  実行したコードは、データが**bronze**
    フォルダ内のCSVファイルからSparkデータフレームにロードされてからデータフレームの最初の数行を表示しました。　

> **注:
> 出力ペインの左上にある…メニューを選択すると、セル出力の内容をクリア、非表示、自動サイズ変更できます。**

8.  次に、PySparkデータフレームを使用して、データ検証とクリーンアップのための列を追加し、既存の列の値を更新します。+ボタンを使用して新しいコードブロックを追加し、次のコードをセルに追加します:

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
> コードの最初の行は、PySparkから必要な関数をインポートします。次に、データフレームに新しい列を追加して、ソースファイル名、注文が対象会計年度以前のものとしてフラグ付けされているかどうか、行がいつ作成および変更されたかを追跡できるようにします。
>
> 最後に、CustomerName 列が null または空の場合は "Unknown"
> に更新します。
>
> Then, Run the cell to execute the code using the 次に、**\*\*▷** (*Run
> cell*)\*\*ボタンを使用し、セルを実行し、コードを実行する 。

![A screenshot of a computer Description automatically
generated](./media/image29.png)

![A screenshot of a computer Description automatically
generated](./media/image30.png)

9.  次に、Delta
    Lake形式を使用して、salesデータベースのsales_silverテーブルのスキーマを定義します。新しいコードブロックを作成し、セルに次のコードを追加します:

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

   

10. **\*\*▷** (*Run cell*)\*\*
    ボタンを使用してセルを実行し、コードを実行します。

11. Lakehouse explorerペーンにTablesのセクションにある**…**
    を選択して**Refresh**を選択する。これで、リストされた新しい**sales_silverテーブルが表示できます。** **▲** (三角アイコン)が、Deltaテーブルであることを指定します。.

> **注:
> 新しいテーブルが表示されない場合は、数秒待ってからもう一度\[Refresh\]を選択するか、ブラウザタブ全体を更新します。**.
>
> ![A screenshot of a computer Description automatically
> generated](./media/image31.png)
>
> ![A screenshot of a computer Description automatically
> generated](./media/image32.png)

1.  次に、 Delta テーブルに対して **upsert
    操作**を実行し、特定の条件に基づいて既存のレコードを更新し、一致するレコードが見つからない場合は新しいレコードを挿入します。新しいコードブロックを追加し、次のコードを貼り付けます:

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

12. セルを実行し、**\*\*▷**(*Run
    cell*)\*\*ボタンを使用してコードを実行します.

![A screenshot of a computer Description automatically
generated](./media/image33.png)

この操作は、特定の列の値に基づいてテーブル内の既存のレコードを更新し、一致するものが見つからない場合は新しいレコードを挿入できるため重要です。これは、既存レコードと新規レコードの更新が含まれる可能性のあるソースシステムからデータをロードする場合によく必要な要件です。

これで、シルバーデルタテーブルにデータが保存され、さらに変換とモデリングを行う準備が整いました。

これで、Bronze レイヤーからデータを通常に取得して変換し、Silver Delta
テーブルにロードしました。新しいノートブックを使用してデータをさらに変換し、スター
スキーマにモデル化し、Gold Delta テーブルにロードします。

*Note that you could have done all of this in a single notebook, but for
the purposes of this 手順 you’re using separate notebooks to demonstrate
the process of transforming data from bronze to silver and then from
silver to gold. This can help with debugging, troubleshooting, and
reuse*.

## **タスク 2: Gold Delta テーブルへのデータの読み込み**

1.  Fabric Lakehouse Tutorial-29 ホーム ページに戻る.

> ![A screenshot of a computer Description automatically
> generated](./media/image34.png)

2.  **wwilakehouseを選択する**

![A screenshot of a computer Description automatically
generated](./media/image35.png)

3.  In the lakehouse explorerペーン内に 、ExplorerペーンにTables
    セクションにリストされたsales_silver テーブルを表示します。

![A screenshot of a computer Description automatically
generated](./media/image36.png)

4.  次に、**Transform data for
    Goldと名前つけられたノートブックをを作成します。**このため、コマンドバーにある**Open
    notebookのドロップに移動してクリックしてからNew
    notebook**を選択ｓるう、

![A screenshot of a computer Description automatically
generated](./media/image37.png)

> 既存のコード
> ブロックで、定型テキストを削除し、次のコードを追加してデータフレームにデータを読み込み、スター
> スキーマの構築を開始して実行します：
>
> CodeCopy

\# Load data to the dataframe as a starting point to create the gold
layer

df = spark.read.table("wwilakehouse.sales_silver")

\# Display the first few rows of the dataframe to verify the data

df.show()

![A screenshot of a computer Description automatically
generated](./media/image38.png)

5.  次に、新しいコード
    ブロックを追加し、次のコードを貼り付けて日付ディメンション
    テーブルを作成し、実行します:

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

注:
作業の進捗状況を確認するには、いつでもdisplay(df) コマンドを実行できます。この場合、‘display(dfdimDate_gold)’を実行して、dimDate_goldデータフレームの内容を確認します。

6.  新しいコードブロックに**次のコードを追加して実行し**、日付ディメンション
    **dimdate_gold** のデータフレームを作成します:

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

> コードを新しいコードブロックに分割することで、データを変換する際にノートブック内で何が起こっているかを把握し、監視できるようになります。別の新しいコードブロックに、新しいデータが入力されたときに日付ディメンションを更新する**次のコードを追加して実行**します。CodeCopy
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

> 日付ディメンションの設定が完了しました。

![A screenshot of a computer Description automatically
generated](./media/image43.png)

## **タスク 3: 顧客ディメンションを作成する。**

1.  顧客ディメンション テーブルを作成するには、**新しいコード
    ブロックを追加し**、次のコードを貼り付けて実行します:

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

2.  新しいコード
    ブロックに**次のコードを追加して実行**し、重複する顧客を削除し、特定の列を選択し、「CustomerName」列を分割して「First」と「Last」の名前列を作成します。

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

ここでは、重複の削除、特定の列の選択、「CustomerName」列を分割して「First」と「Last」の列を作成するなど、さまざまな変換を実行して、新しいデータフレーム
dfdimCustomer_silver
を作成しました。結果は、「CustomerName」列から抽出された「First」と「Last」の列を含む、クリーンアップされ構造化された顧客データを含むデータフレームです。

![A screenshot of a computer Description automatically
generated](./media/image47.png)

3.  次に、顧客のID列を作成します。新しいコードブロックに以下を貼り付けて実行します:

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

4.  新しいデータが入ってくると顧客テーブルが最新の状態に保たれることを確認します。**新しいコードブロック**に次のコードを貼り付けて実行します。:

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

5.  次に、**これらの手順を繰り返して、商品ディメンションを作成します**。新しいコードブロックに、次のものを貼り付けて実行します:

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

6.  **product_silver**データフレームを作成するために**別のコードブロックを追加します。**.

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

7.  次に、dimProduct_goldテーブルのIDを作成します。次の構文を新しいコードブロックに追加して実行します:

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

これにより、テーブル内の現在のデータに基づいて次に使用可能な製品 ID
が計算され、これらの新しい ID
が製品に割り当て、更新された製品情報が表示されます。

![A screenshot of a computer Description automatically
generated](./media/image57.png)

8.  他のディメンションで行ったのと同様に、新しいデータが入ってくるたびに製品テーブルが最新の状態に保たれるようにする必要があります。新しいコード
    ブロックに次のコードを貼り付けて実行します：

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

**ディメンションが構築されたので、最後のステップはファクト
テーブルを作成することです。**

1.  **新しいコード
    ブロックに**、次のコードを貼り付けて実行し、**ファクト
    テーブルを作成します**。

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

9.  **新しいコード ブロック**に次のコードを貼り付けて実行し、顧客
    ID、商品
    ID、注文日、数量、単価、税金などの顧客情報と商品情報を結合する**新しいデータフレーム**を作成します:

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

1.  次に、**新しいコードブロック**で次のコードを実行して、売上データが最新の状態に保たれていることを確認します:

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

ここでは、Delta
Lakeのマージ操作を使用して、factsales_goldテーブルを新しい売上データ（dffactSales_gold）と同期・更新しています。この操作では、既存のデータ（silverテーブル）と新しいデータ（DataFrameを更新）の注文日、顧客ID、商品IDを比較し、一致するレコードを更新し、必要に応じて新しいレコードを挿入します。

![A screenshot of a computer Description automatically
generated](./media/image66.png)

これで、レポート作成や分析に使用できる、キュレーションされモデル化された**ゴールド**レイヤーが完成しました。

# 手順 4: Azure Databricks と Azure Data Lake Storage (ADLS) Gen 2 間の接続を確立する

それでは、Azure Databricksを使用してAzure Data Lake Storage (ADLS)
Gen2アカウントでDeltaテーブルを作成しましょう。次に、ADLSのDeltaテーブルへのOneLakeショートカットを作成し、Power
BIを使用してADLSショートカット経由でデータを分析します。

**タスク 0: Azure パスを利用して Azure サブスクリプションを有効にする**

## 

1.  次のリンク !!https://www.microsoftazurepass.com/!!
    に移動し、\[Start\] ボタンをクリックします。

![](./media/image67.png)

2.  Microsoft サインインページでテナント ID を入力し、\[Next\]
    をクリックします。

![](./media/image68.png)

1.  次のページでパスワードを入力し、\[**サインイン**\]をクリックします。

![](./media/image69.png)

![A screenshot of a computer error Description automatically
generated](./media/image70.png)

3.  ログインしたら、Microsoft Azure ページで \[Microsoft
    アカウントの確認\] タブをクリックします。

![](./media/image71.png)

4.  次のページで、プロモーションコード、キャプチャ文字を入力し、\[Submit\]をクリックします
    **。**

![A screenshot of a computer Description automatically
generated](./media/image72.png)

![A screenshot of a computer error Description automatically
generated](./media/image73.png)

5.  \[Your
    profile\]ページでプロフィールの詳細を入力し、\[Sign-up\]をクリックします
    **。**

6.  プロンプトが表示されたら、多要素認証にサインアップし、次のリンクに移動して
    Azure ポータルにログインします !! <https://portal.azure.com/#home>!!

![](./media/image74.png)

7.  検索バーに「サブスクリプション」と入力し、\[Service\]の下にある\[Subscription\]アイコンをクリックします
    **.**

![A screenshot of a computer Description automatically
generated](./media/image75.png)

8.  Azure パスの引き換えに成功すると、サブスクリプション ID
    が生成されます。

![](./media/image76.png)

## **タスク 1: Azure Data Storage アカウントを作成する**

1.  Azure の資格情報を使用して、Azure portal にサインインします。

2.  ホーム ページで、左側のポータル メニューから \[**Storage
    accounts**\] を選択して、ストレージ
    アカウントの一覧を表示します。ポータル
    メニューが表示されない場合は、メニュー
    ボタンを選択してオンに切り替えます.

![A screenshot of a computer Description automatically
generated](./media/image77.png)

3.  \[**Storage accounts**\] ページで、 \[**Create\] を選択します**.

![A screenshot of a computer Description automatically
generated](./media/image78.png)

4.  \[基本\] タブでリソース グループを選択し、ストレージ
    アカウントの必須情報を入力します：

[TABLE]

他の設定はそのままにして、\[**Review+create**\]
を選択し、デフォルトのオプションを受け入れて、アカウントの検証と作成に進みます。

注: リソース グループをまだ作成していない場合は、\[**Create new**\]
をクリックして、ストレージ アカウントの新しいリソースを作成できます.

![](./media/image79.png)

![](./media/image80.png)

![A screenshot of a computer Description automatically
generated](./media/image81.png)

![A screenshot of a computer Description automatically
generated](./media/image82.png)

![A screenshot of a computer Description automatically
generated](./media/image83.png)

「Review+create」タブに移動すると、Azure は選択したストレージ
アカウント設定の検証を実行します。検証に合格すると、ストレージ
アカウントの作成に進むことができます。検証に失敗した場合は、ポータルに変更が必要な設定が表示されます。.

![A screenshot of a computer Description automatically
generated](./media/image84.png)

![A screenshot of a computer Description automatically
generated](./media/image85.png)

これで、Azure データ ストレージ アカウントが正常に作成されました。

5.  ページ上部の検索バーで検索してストレージアカウントページに移動し、新しく作成したストレージアカウントを選択します。

![A screenshot of a computer Description automatically
generated](./media/image86.png)

6.  ストレージ アカウント ページで、左側のナビゲーション ペインの
    \[**Data storage**\] の下の \[**Containers**\]
    に移動し、!!medalion1!!
    という名前の新しいコンテナーを作成して、\[**Create**\]
    ボタンをクリックします。 

 

![A screenshot of a computer Description automatically
generated](./media/image87.png)

7.  **ストレージ**アカウントページに戻り、左側のナビゲーションメニューから「**エンドポイント**」を選択します。下にスクロールしてプライマリエンドポイント**Primary
    endpoint**のURLをコピーし、メモ帳に貼り付けます。これはショートカットを作成する際に役立ちます。

![](./media/image88.png)

8.  同様に、同じナビゲーションパネルの**アクセスキー**に移動します。

![A screenshot of a computer Description automatically
generated](./media/image89.png)

## **タスク 2: Delta テーブルを作成し、ショートカットを作成し、Lakehouse 内のデータを分析します**

1.  Lakehouseで、ファイルの横にある**(...)** を選択し、 \[**New
    shortcut\]** を選択します。

![](./media/image90.png)

2.  **New shortcut画面にAzure Data Lake Storage Gen2**タイルを選択する。

![Screenshot of the tile options in the New shortcut
screen.](./media/image91.png)

3.  ショートカットの接続の詳細を指定する:

[TABLE]

4.  **Next**をクリックする

![A screenshot of a computer Description automatically
generated](./media/image92.png)

5.  これにより、Azure ストレージ
    コンテナーとのリンクが確立されます。ストレージを選択し、\[**次へ**\]
    ボタンを選択します。

![A screenshot of a computer Description automatically
generated](./media/image93.png)

![A screenshot of a computer Description automatically
generated](./media/image94.png)![A screenshot of a computer Description
automatically generated](./media/image95.png)

6.  ウィザードが起動したら、「**Files**」を選択し、**ブロンズ**ファイルの「…」を選択します。

![A screenshot of a computer Description automatically
generated](./media/image96.png)

7.  **Load to tablesとnew table**を選択する。

![](./media/image97.png)

8.  ポップアップウィンドウで、テーブルの名前**を bronze_01** として指定
    し、ファイルの種類として **parquet** を選択します。

![A screenshot of a computer Description automatically
generated](./media/image98.png)

![A screenshot of a computer Description automatically
generated](./media/image99.png)

![A screenshot of a computer Description automatically
generated](./media/image100.png)

![A screenshot of a computer Description automatically
generated](./media/image101.png)

9.  ファイル**bronze_01**がファイル内に表示されるようになりました。

![A screenshot of a computer Description automatically
generated](./media/image102.png)

10. 次に、**ブロンズ**ファイルの「…」を選択します。「**Load to
    tables**」と「**Existing table**」を選択します

![A screenshot of a computer Description automatically
generated](./media/image103.png)

11. 既存のテーブル名を**dimcustomer_gold**として提供する。ファイルの種類は**parquet**と選択し、**load**を選択します**.**

![A screenshot of a computer Description automatically
generated](./media/image104.png)

![A screenshot of a computer Description automatically
generated](./media/image105.png)

## **タスク 3: レポートを作成するためにゴールドレイヤーを使用してセマンティックモデルを作成する**

ワークスペースでは、ゴールドレイヤーを使用してレポートを作成し、データを分析できるようになりました。ワークスペース内でセマンティックモデルに直接アクセスして、レポート用のリレーションシップとメジャーを作成できます。

*Lakehouseを作成すると自動的に作成される**デフォルトのセマンティックモデル**は使用できません。Lakehouse
explorerから、このラボで作成したゴールドテーブルを含む新しいセマンティックモデルを、作成する必要があります。*

1.  ワークスペースで、**wwilakehouse** Lakehouseに移動します。次に
    **、**Lakehouseエクスプローラービューのリボンから\[**New semantic
    model**\] を選択します.

![A screenshot of a computer Description automatically
generated](./media/image106.png)

2.  ポップアップで、 **新**しいセマンティック モデルに
    **DatabricksTutorial** という名前を割り当て、ワークスペースを
    **Fabric Lakehouse Tutorial-29** として選択します。

![](./media/image107.png)

3.  次に、下にスクロールして、セマンティック
    モデルに含めるすべてを選択し、 \[Confirm\] を選択します .

これにより、Fabric でセマンティック
モデルが開き、次に示すようにリレーションシップとメジャーを作成できます:

![A screenshot of a computer Description automatically
generated](./media/image108.png)

ここから、あなたまたはデータチームの他のメンバーは、Lakehouse内のデータに基づいてレポートとダッシュボードを作成できます。これらのレポートはLakehouseのゴールドレイヤーに直接接続されるため、常に最新のデータが反映されます.

# 手順 5: Azure Databricks を使用したデータの取り込みと分析

1.  Power BI サービスでLakehouseに移動し、 \[**Get data**\] を選択し、
    \[**New data pipeline\]** を選択します。

![Screenshot showing how to navigate to new data pipeline option from
within the UI.](./media/image109.png)

**New Pipeline**のプロンプトで、新しいパイプラインの名前「**Ingest Data
pipeline
01**」を入力し、「**Create**」を選択します。![](./media/image110.png)

1.  この手順では、データ ソースとして **NYC Taxi - Green** サンプル
    データを選択します。

![A screenshot of a computer Description automatically
generated](./media/image111.png)

2.  プレビュー画面で、\[**Next**\] を選択します。

![A screenshot of a computer Description automatically
generated](./media/image112.png)

3.  \[**Data destination**\] で、OneLake Delta テーブル
    データの保存に使用するテーブルの名前を選択します。既存のテーブルを選択するか、新しいテーブルを作成できます。このラボでは、
    \[**Load to new table\]** を選択し、 \[**Next**\] を選択します。

![A screenshot of a computer Description automatically
generated](./media/image113.png)

4.  On the **Review + Save画面でStart data transfer
    immediatelyを選択してからSave + Run**を選択する。

![Screenshot showing how to enter table name.](./media/image114.png)

![A screenshot of a computer Description automatically
generated](./media/image115.png)

5.  ジョブが完了したら、Lakehouseに移動し、/Tables
    の下にリストされているデルタ テーブルを表示します。

![A screenshot of a computer Description automatically
generated](./media/image116.png)

6.  Azure Blob Filesystem (ABFS) パスをデルタ
    テーブルにコピーするために、エクスプローラー
    ビューでテーブル名を右クリックし、 \[**プロパティ**\] を選択します。

![A screenshot of a computer Description automatically
generated](./media/image117.png)

7.  Azure Databricks ノートブックを開き、コードを実行します。

olsPath = "**abfss://\<replace with workspace
name\>@onelake.dfs.fabric.microsoft.com/\<replace with item
name\>.Lakehouse/Tables/nycsample**"

df=spark.read.format('delta').option("inferSchema","true").load(olsPath)

df.show(5)

*注:太字のファイルパスをコピーしたものに置き換える。*

![](./media/image118.png)

8.  フィールド値を変更して Delta テーブル データを更新する.

%sql

update delta.\`abfss://\<replace with workspace
name\>@onelake.dfs.fabric.microsoft.com/\<replace with item
name\>.Lakehouse/Tables/nycsample\` set vendorID = 99999 where vendorID
= 1;

*注:太字のファイルパスをコピーしたものに置き換えます。*

![A screenshot of a computer Description automatically
generated](./media/image119.png)

# 手順 6: リソースの削除

この手順では、Microsoft Fabric Lakehouseでメダリオン
アーキテクチャを作成する方法を学習しました。

Lakehouseの探索が終了したら、この手順用に作成したワークスペースを削除できます。

1.  左側のナビゲーション メニューからワークスペース (**Fabric Lakehouse
    Tutorial-29)** を選択します。ワークスペース項目ビューが開きます。

![A screenshot of a computer Description automatically
generated](./media/image120.png)

2.  ワークスペース名の下の「...」オプションを選択し、「**Workspace
    settings**」を選択します。

![A screenshot of a computer Description automatically
generated](./media/image121.png)

3.  一番下までスクロールし、 \[ **Remove this workspace\]**
    を選択します。

![A screenshot of a computer Description automatically
generated](./media/image122.png)

4.  ポップアップする警告で**\[Delete**\]をクリックします。

![A white background with black text Description automatically
generated](./media/image123.png)

5.  ワークスペースが削除されたという通知を待ってから、次のラボに進みます。

![A screenshot of a computer Description automatically
generated](./media/image124.png)

**要約**:

このラボでは、ノートブックを使用してMicrosoft
FabricLakehouseでメダリオンアーキテクチャを構築する方法を学習します。主な手順としては、ワークスペースの設定、Lakehouseの構築、初期取り込みのためのブロンズレイヤーへのデータのアップロード、構造化処理のためのシルバーDeltaテーブルへの変換、高度な分析のためのゴールドDeltaテーブルへの変換、セマンティックモデルの探索、そして洞察に富んだ分析のためのデータリレーションシップの作成などが挙げられます。

## 
