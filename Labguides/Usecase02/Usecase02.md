**介紹**

Apache Spark
是一個用於分布式數據處理的開源引擎，廣泛用於探索、處理和分析 Data Lake
Storage 中的大量數據。Spark 在許多數據平臺產品中作為處理選項提供，包括
Azure HDInsight、Azure Databricks、Azure Synapse Analytics 和 Microsoft
Fabric。Spark 的好處之一是支持多種編程語言，包括 Java、Scala、Python 和
SQL;使 Spark
成為數據處理工作負載的非常靈活的解決方案，包括數據清理和作、統計分析和機器學習以及數據分析和可視化。

Microsoft Fabric Lakehouse 中的表基於 Apache Spark 的開源 *Delta Lake*
格式。Delta Lake 增加了對批處理和流數據作的關係語義的支持，並支持創建
Lakehouse 體系結構，在該體系結構中，Apache Spark
可用於處理和查詢基於數據湖中基礎文件的表中的數據。

在 Microsoft Fabric 中，數據流 （Gen2） 連接到各種數據源，並在 Power
Query Online 中執行轉換。然後，可以在 Data Pipelines
中使用它們將數據引入 Lakehouse 或其他分析存儲，或為 Power BI
報表定義數據集。

此實驗室旨在介紹 Dataflows （Gen2）
的不同元素，而不是創建企業中可能存在的複雜解決方案。

**目標：**

- 在 Microsoft Fabric 中創建工作區，並啟用 Fabric 試用版。

- 建立湖倉一體環境並上傳數據文件進行分析。

- 生成用於交互式數據探索和分析的筆記本。

- 將數據加載到 DataFrame 中，以便進一步處理和可視化。

- 使用 PySpark 對數據應用轉換。

- 保存轉換後的數據並對其進行分區，以實現優化查詢。

- 在 Spark 元存儲中創建表以進行結構化數據管理

&nbsp;

- 將 DataFrame 另存為名為 “salesorders” 的託管增量表。

- 將 DataFrame 另存為具有指定路徑的名為 “external_salesorder” 的外部
  delta 表。

- 描述和比較託管表和外部表的屬性。

- 對表執行 SQL 查詢以進行分析和報告。

- 使用 Python 庫（如 matplotlib 和 seaborn）可視化數據。

- 在數據工程體驗中建立數據湖倉一體，並攝取相關數據以供後續分析。

- 定義用於提取、轉換數據並將其加載到 Lakehouse 中的數據流。

- 在 Power Query 中配置數據目標，以將轉換後的數據存儲在 Lakehouse 中。

- 將數據流合併到管道中，以啟用計劃的數據處理和攝取。

- 刪除工作區和關聯的元素以結束練習。

# 練習 1：創建 workspace、Lakehouse、Notebook 並將數據加載到 DataFrame 中 

## 任務 1：創建工作區 

在 Fabric 中處理數據之前，請創建一個啟用了 Fabric 試用版的工作區。

1.  打開瀏覽器，導航到地址欄，然後鍵入或粘貼以下
    URL：<https://app.fabric.microsoft.com/> 然後按 **Enter** 按鈕。

> **注意**：如果您被定向到 Microsoft Fabric 主頁，請跳過從 \#2 到 \#4
> 的步驟。
>
> ![](./media/image1.png)

2.  在 **Microsoft Fabric** 窗口中，輸入您的憑據，然後單擊 **Submit**
    按鈕。

> ![](./media/image2.png)

3.  然後，在 **Microsoft** 窗口中輸入密碼並單擊 **Sign in** 按鈕**。**

> ![A login screen with a red box and blue text Description
> automatically generated](./media/image3.png)

4.  在 **Stay signed in?** 窗口中，單擊 **Yes** 按鈕。

> ![A screenshot of a computer error Description automatically
> generated](./media/image4.png)

5.  Fabric 主頁，選擇 **+New workspace tile**。

> ![A screenshot of a computer AI-generated content may be
> incorrect.](./media/image5.png)

6.  在 **Create a workspace tab** 中，輸入以下詳細信息，然後單擊
    **Apply** 按鈕。

[TABLE]

> ![A screenshot of a computer Description automatically
> generated](./media/image6.png)![](./media/image7.png)![](./media/image8.png)![](./media/image9.png)

7.  等待部署完成。完成需要 2-3 分鐘。當您的新 workspace
    打開時，它應該是空的。

## 任務 2：創建 Lakehouse 並上傳文件

現在，你已擁有工作區，可以切換到門戶中的 *Data engineering*
體驗，並為要分析的數據文件創建數據湖倉一體。

1.  通過單擊導航欄中的 **+New item** 按鈕創建新的 Eventhouse。

![A screenshot of a browser AI-generated content may be
incorrect.](./media/image10.png)

2.  點擊“**Lakehouse”**磁貼。

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image11.png)

3.  在 **New lakehouse** 對話框中， 在 **Name** 字段中輸入
    **+++Fabric_lakehouse+++ **，單擊 **Create** 按鈕並打開新的
    Lakehouse。

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image12.png)

4.  大約一分鐘後，將創建一個新的空
    Lakehouse。您需要將一些數據提取到數據湖倉一體中進行分析。

![A screenshot of a computer Description automatically
generated](./media/image13.png)

5.  您將看到一條通知，指出 **Successfully created SQL endpoint**。

> ![](./media/image14.png)

6.  在 **Explorer** 部分的 **fabric_lakehouse** 下，將鼠標懸停在 **Files
    folder** 旁邊，然後單擊水平省略號 **（...）** 菜單。導航並單擊
    **Upload**，然後單擊 **Upload folder**，如下圖所示。

![](./media/image15.png)

7.  在右側顯示的 **Upload folder** 窗格中，選擇 **Files/** 下的 **folder
    icon**，然後瀏覽到 **C：\LabFiles**，然後選擇 **orders**
    文件夾並單擊 Upload 按鈕。

![](./media/image16.png)

8.  如果上傳 **Upload 3 files to this site?** 對話框，然後單擊
    **Upload** 按鈕。

![](./media/image17.png)

9.  在 Upload folder 窗格中，單擊 **Upload** 按鈕。

> ![](./media/image18.png)

10. 上傳文件後，**關閉 Upload folder** 窗格。

> ![](./media/image19.png)

11. 展開 **Files** 並選擇 **orders** 文件夾，並驗證 CSV 文件是否已上傳。

![](./media/image20.png)

## 任務 3：創建 notebook

要在 Apache Spark
中處理數據，您可以創建一個*筆記本*。筆記本提供了一個交互式環境，您可以在其中編寫和運行代碼（以多種語言），並添加注釋以記錄代碼。

1.  在 **Home** 上，查看數據湖中 **orders** 文件夾的內容時，在 **Open
    notebook** 菜單中，選擇 **New notebook**。

![](./media/image21.png)

2.  幾秒鐘後，將打開一個包含單個單元格的新筆記本
    。筆記本由一個或多個單元格組成，這些單元格可以包含*代碼*或
    *Markdown*（格式化文本）。

![](./media/image22.png)

3.  選擇第一個單元格（當前為*代碼*單元格），然後在其右上角的動態工具欄中，使用
    **M↓** 按鈕 **convert the cell to a markdown cell**。

![](./media/image23.png)

4.  當單元格更改為 Markdown 單元格時，將呈現它包含的文本。

![](./media/image24.png)

5.  **🖉** 使用 （Edit）
    按鈕將單元格切換到編輯模式，替換所有文本，然後按如下方式修改
    markdown:

> CodeCopy
>
> \# Sales order data exploration
>
> Use the code in this notebook to explore sales order data.

![](./media/image25.png)

![A screenshot of a computer Description automatically
generated](./media/image26.png)

6.  單擊筆記本中單元格外部的任意位置可停止編輯它並查看呈現的 Markdown。

![A screenshot of a computer Description automatically
generated](./media/image27.png)

## 任務 4：將數據加載到 DataFrame 中

現在，您可以運行將數據加載到 *DataFrame* 中的代碼。Spark
中的數據幀類似於 Python 中的 Pandas
數據幀，並提供一種通用結構來處理行和列中的數據。

**注意**：Spark 支持多種編碼語言，包括 Scala、Java
等。在本練習中，我們將使用 *PySpark*，它是 Spark 優化的 Python
變體。PySpark 是 Spark 上最常用的語言之一，也是 Fabric
筆記本中的默認語言。

1.  在筆記本可見的情況下，展開 **Files** 列表並選擇 **orders**
    文件夾，以便 CSV 文件列在筆記本編輯器旁邊。

![](./media/image28.png)

2.  現在，您的鼠標2019.csv文件。單擊水平省略號 **（...）**
    除了2019.csv。導航並單擊 **Load data**，然後選擇
    **Spark**。包含以下代碼的新代碼單元格將添加到筆記本中：

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

**提示**： 您可以使用左側的 « 圖標來隱藏左側的 Lakehouse Explorer 窗格
。行為

所以將幫助您專注於筆記本。

3.  使用單元格左側的 **▷ Run cell** 按鈕來運行它。

![](./media/image31.png)

**注意**：由於這是您第一次運行任何 Spark 代碼，因此必須啟動 Spark
會話。這意味著會話中的第一次運行可能需要一分鐘左右才能完成。後續運行會更快。

4.  cell 命令完成後，查看單元格下方的輸出，該輸出應類似於以下內容：

![](./media/image32.png)

5.  輸出顯示 2019.csv
    文件中數據的行和列。但是，請注意，列標題看起來不正確。用於將數據加載到
    DataFrame 的默認代碼假定 CSV
    文件在第一行中包含列名，但在這種情況下，CSV
    文件僅包含數據，而不包含標題信息。

6.  修改代碼以將 **header** 選項設置為 **false**。將單元格中的所有代碼
    替換為以下代碼，然後單擊 **▷ Run cell** 按鈕並查看輸出

> CodeCopy
>
> df =
> spark.read.format("csv").option("header","false").load("Files/orders/2019.csv")
>
> \# df now is a Spark DataFrame containing CSV data from
> "Files/orders/2019.csv".
>
> display(df)

![](./media/image33.png)

7.  現在，dataframe
    正確地包含第一行作為數據值，但列名是自動生成的，不是很有幫助。要理解數據，您需要為文件中的數據值顯式定義正確的架構和數據類型。

8.  將 **cell** 中的所有代碼 替換為以下代碼，然後單擊 **▷ Run cell**
    按鈕並查看輸出

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

![](./media/image35.png)

9.  現在 DataFrame 包含正確的列名（除了 **Index，**它是所有 DataFrame
    中的內置列，基於每行的序號位置）。列的數據類型是使用 Spark SQL
    庫中定義的一組標準類型指定的，這些類型是在單元格的開頭導入的。

10. 通過查看數據幀確認您的更改已應用於數據。

11. 使用 單元格輸出下方的 **+ Code**
    圖標將新的代碼單元格添加到筆記本中，然後在其中輸入以下代碼。點擊 **▷
    Run cell** 按鈕並查看輸出

> CodeCopy
>
> display(df)
>
> ![A screenshot of a computer Description automatically
> generated](./media/image36.png)

12. 數據幀僅包含 **2019.csv** 文件中的數據。修改代碼，以便文件路徑使用
    \* 通配符從 orders 文件夾中的所有文件中讀取銷售訂單數據

13. 使用 單元格輸出下方的 **+ Code**
    圖標將新的代碼單元格添加到筆記本中，然後在其中輸入以下代碼。

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
>
> display(df)

![](./media/image37.png)

14. 運行修改後的代碼單元並查看輸出，現在應包括 2019 年、2020 年和 2021
    年的銷售額。

![](./media/image38.png)

**注意**：僅顯示行的子集，因此您可能無法看到所有年份的示例。

# 練習 2：瀏覽 dataframe 中的數據

dataframe
對象包含各種函數，您可以使用這些函數來篩選、分組和以其他方式作它所包含的數據。

## 任務 1：篩選 dataframe

1.  使用 單元格輸出下方的 **+ Code**
    圖標將新的代碼單元格添加到筆記本中，然後在其中輸入以下代碼。

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

2.  **Run** 新的代碼單元，並查看結果。請注意以下詳細信息:

    - 當您對 DataFrame 執行作時，結果是一個新的
      DataFrame（在本例中，通過從 **df** DataFrame
      中選擇特定的列子集來創建新的 **customers** DataFrame）

    &nbsp;

    - Dataframes 提供 **count** 和 **distinct**
      等函數，可用於匯總和篩選它們包含的數據。

    &nbsp;

    - dataframe\['Field1'， 'Field2'， ...\]
      語法是定義列子集的簡寫方法。您還可以使用 **select**
      方法，因此上面代碼的第一行可以寫成 customers =
      df.select（“CustomerName”， “Email”）

> ![](./media/image40.png)

3.  修改代碼，將 **cell** 中的所有代碼替換為 以下代碼，然後單擊 **▷ Run
    cell** 按鈕，如下所示：

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

4.  **Run運行**修改後的代碼以查看已購買 ***Road-250 Red， 52* product**
    的客戶。請注意，您可以將多個函數 **“chain”**
    在一起，以便一個函數的輸出成為下一個函數的輸入 -
    在這種情況下，**select** 方法創建的數據幀是用於應用篩選條件的
    **where** 方法的源數據幀。

> ![](./media/image41.png)

## 任務 2：在 dataframe 中聚合和分組數據

1.  單擊 **+ Code** **+** 並複製並粘貼以下代碼，然後單擊 **Run cell**
    按鈕。

CodeCopy

> productSales = df.select("Item", "Quantity").groupBy("Item").sum()
>
> display(productSales)
>
> ![](./media/image42.png)

2.  請注意，結果顯示按產品分組的訂單數量總和。**groupBy** 方法按 Item
    對行進行分組，隨後的 **sum**
    聚合函數將應用於所有剩餘的數字列（在本例中為 *Quantity*）

![](./media/image43.png)

3.  單擊 **+ Code** 並複製並粘貼以下代碼，然後單擊 **Run cell** 按鈕。

> **CodeCopy**
>
> from pyspark.sql.functions import \*
>
> yearlySales =
> df.select(year("OrderDate").alias("Year")).groupBy("Year").count().orderBy("Year")
>
> display(yearlySales)

![](./media/image44.png)

4.  請注意，結果顯示每年的銷售訂單數量。請注意，**select** 方法包括一個
    SQL **year** 函數，用於提取 *OrderDate*
    字段的年份部分（這就是代碼包含一個 **import** 語句以從 Spark SQL
    庫導入函數的原因）。然後，它使用** alias**
    方法為提取的年份值分配列名稱。然後，按派生的 Year 列對數據進行分組
    ，並計算每個組中的行數，最後使用 **orderBy**
    方法對生成的數據幀進行排序。

![](./media/image45.png)

# 練習 3：使用 Spark 轉換數據文件

數據工程師的一項常見任務是以特定格式或結構攝取數據，並對其進行轉換以進行進一步的下游處理或分析。

## 任務 1：使用 DataFrame 方法和函數轉換數據

1.  單擊 + Code 並複製並粘貼以下代碼

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

1.  **Run** 代碼，通過以下轉換從原始訂單數據創建新的 DataFrame:

    - 根據 **OrderDate** 列添加 **Year** 和 **Month** 列。

    - 根據 **CustomerName** 列添加 **FirstName** 和 **LastName** 列。

    - 篩選並重新排序列，刪除 **CustomerName** 列。

![](./media/image47.png)

2.  查看輸出並驗證是否已對數據進行轉換。

![](./media/image48.png)

您可以使用 Spark SQL
庫的全部功能，通過篩選行、派生、刪除、重命名列以及應用任何其他所需的數據修改來轉換數據。

**提示**：請參閱 [*Spark DataFrame
documentation*](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html)，瞭解有關
Dataframe 對象方法的更多信息。

## 任務 2：保存轉換後的數據

1.  使用以下代碼 **Add a new cell**，以 Parquet
    格式保存轉換後的數據幀（如果數據已存在，則覆蓋數據）。**Run**
    單元格並等待數據已保存的消息。

> CodeCopy
>
> transformed_df.write.mode("overwrite").parquet('Files/transformed_data/orders')
>
> print ("Transformed data saved!")
>
> **注意**：通常，*Parquet*
> 格式是用於進一步分析或引入分析存儲的數據文件的首選格式。Parquet
> 是一種非常有效的格式，大多數大型數據分析系統都支持它。事實上，有時您的數據轉換要求可能只是將數據從其他格式（例如
> CSV）轉換為 Parquet！

![](./media/image49.png)

![](./media/image50.png)

2.  然後，在左側的 **Lakehouse explorer** 窗格中，在 **Files** 節點的
    ... 菜單中，選擇**Refresh**。

> ![](./media/image51.png)

3.  單擊 **transformed_data** 文件夾以驗證它是否包含名為 **orders**
    的新文件夾，而該文件夾又包含一個或多個 **Parquet files**。

![](./media/image52.png)

4.  單擊 **+ Code** following code**，**從 **transformed_data -\>
    orders** 文件夾中的 parquet 文件加載新的數據幀 :

> **CodeCopy**
>
> orders_df =
> spark.read.format("parquet").load("Files/transformed_data/orders")
>
> display(orders_df)
>
> ![](./media/image53.png)

5.  **Run** 單元格並驗證結果是否顯示已從 parquet 文件加載的訂單數據。

> ![](./media/image54.png)

## 任務 3：將數據保存在分區文件中

1.  添加一個新單元格，單擊使用以下代碼的 **+ Code** ;這將保存
    DataFrame，並按 **Year** 和**Month** 對數據進行分區。 **Run**
    單元格並等待數據已保存的消息

> CodeCopy
>
> orders_df.write.partitionBy("Year","Month").mode("overwrite").parquet("Files/partitioned_data")
>
> print ("Transformed data saved!")
>
> ![](./media/image55.png)
>
> ![](./media/image56.png)

2.  然後，在左側的 **Lakehouse** 資源管理器窗格中，在 **Files** 節點的
    ... 菜單中，選擇刷新**Refresh**。

![](./media/image57.png)

![](./media/image58.png)

3.  展開 **partitioned_orders** 文件夾以驗證它是否包含名為 **Year=xxxx**
    的文件夾層次結構，每個文件夾都包含名為 **Month=xxxx**
    的文件夾。每個月份文件夾都包含一個 parquet
    文件，其中包含該月的訂單。

![](./media/image59.png)

![](./media/image60.png)

> 在處理大量數據時，對數據文件進行分區是優化性能的常用方法。此技術可以顯著提高性能，並使其更易於篩選數據。

4.  添加新單元格，單擊包含以下代碼的 **+ Code，**從 **orders.parquet**
    文件加載新數據幀：

> CodeCopy
>
> orders_2021_df =
> spark.read.format("parquet").load("Files/partitioned_data/Year=2021/Month=\*")
>
> display(orders_2021_df)

![](./media/image61.png)

5.  **Run** 單元格並驗證結果是否顯示 2021
    年銷售額的訂單數據。請注意，在路徑中指定的分區列（**Year** 和
    **Month**）不包含在數據幀中。

![](./media/image62.png)

# **練習 3：使用** tables **和 SQL**

正如您所看到的，DataFrame
對象的本機方法使您能夠非常有效地查詢和分析文件中的數據。但是，許多數據分析師更習慣於使用他們可以使用
SQL 語法查詢的表。Spark
提供了一個*metastore *，您可以在其中定義關係表。提供 DataFrame 對象的
Spark SQL 庫還支持使用 SQL 語句來查詢元存儲中的表。通過使用 Spark
的這些功能，您可以將數據湖的靈活性與關係數據倉庫的結構化數據架構和基於
SQL 的查詢相結合，因此稱為“數據湖倉一體”。

## 任務 1：創建託管 table

Spark
元存儲中的表是對數據湖中文件的關係抽象。表可以是*託管*的（在這種情況下，文件由元存儲管理）或*外部*的（在這種情況下，表引用數據湖中您獨立於元存儲管理的文件位置）。

1.  添加新代碼，單擊筆記本的 **+ Code** cell
    並輸入以下代碼，該代碼將銷售訂單數據的數據幀保存為名為
    **salesorders** 的表：

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

**注意**：值得注意的是此示例的幾點。首先，沒有提供顯式路徑，因此表的文件將由
metastore 管理。其次，該表以 **delta**
格式保存。您可以基於多種文件格式（包括 CSV、Parquet、Avro 等）創建表，但
*delta lake* 是一種 Spark
技術，可向表添加關系數據庫功能;包括對事務、行版本控制和其他有用功能的支持。對於
Fabric 中的數據湖倉一體，最好以 delta 格式創建表。

2.  **Run** 代碼單元並查看輸出，其中描述了新表的定義。

![A screenshot of a computer Description automatically
generated](./media/image64.png)

3.  在  **Lakehouse explorer** 窗格中，在 **Tables** 文件夾的 ...
    菜單中，選擇 **Refresh**。

![A screenshot of a computer Description automatically
generated](./media/image65.png)

4.  然後，展開 **Tables** 節點並驗證是否已創建 **salesorders** 表。

> ![A screenshot of a computer Description automatically
> generated](./media/image66.png)

5.  將鼠標懸停在 **salesorders** 表旁邊，然後單擊水平省略號
    （...）。導航並單擊 **Load data**，然後選擇 **Spark**。

![A screenshot of a computer Description automatically
generated](./media/image67.png)

6.  單擊 **▷ Run cell** 按鈕，該按鈕使用 Spark SQL 庫在 PySpark
    代碼中嵌入針對 **salesorder** 表的 SQL
    查詢，並將查詢結果加載到數據幀中。

> CodeCopy
>
> df = spark.sql("SELECT \* FROM \[your_lakehouse\].salesorders LIMIT
> 1000")
>
> display(df)

![A screenshot of a computer program Description automatically
generated](./media/image68.png)

![](./media/image69.png)

## 任務 2：創建 external table

您還可以創建*外部* tables，其中架構元數據在 Lakehouse
的元存儲中定義，但數據文件存儲在外部位置。

1.  在第一個代碼單元格返回的結果下，使用 **+ Code**
    按鈕添加新的代碼單元格（如果尚不存在）。然後在新單元格中輸入以下代碼。

CodeCopy

> df.write.format("delta").saveAsTable("external_salesorder",
> path="\<abfs_path\>/external_salesorder")

![A screenshot of a computer Description automatically
generated](./media/image70.png)

2.  在 **Lakehouse explorer** 窗格中的 **...** 菜單中 ，選擇** Files**
    中的 Copy ABFS path。

> ABFS 路徑是指向 **Lakehouse** 的 OneLake 存儲中 **Files**
> 文件夾的完全限定路徑 - 類似於：

abfss://dp_Fabric29@onelake.dfs.fabric.microsoft.com/Fabric_lakehouse.Lakehouse/Files/external_salesorder

![A screenshot of a computer Description automatically
generated](./media/image71.png)

3.  現在，移動到代碼單元格中，將 **\<abfs_path\>** 替換為
    您複製到記事本的 path，以便代碼將 DataFrame
    保存為外部表，其中包含數據文件，位於 **Files** 文件夾**path** 名為
    **external_salesorder** 的文件夾中。完整路徑應類似於

abfss://dp_Fabric29@onelake.dfs.fabric.microsoft.com/Fabric_lakehouse.Lakehouse/Files/external_salesorder

4.  使用單元格左側的 **▷ （*Run cell*）** 按鈕運行它。

![](./media/image72.png)

5.  在 **Lakehouse explorer** 窗格中，在 **Tables** 文件夾的 ...
    菜單中，選擇 **Refresh**。

![A screenshot of a computer Description automatically
generated](./media/image73.png)

6.  然後展開 **Tables** 節點並驗證是否已創建 **external_salesorder**
    表。

![A screenshot of a computer Description automatically
generated](./media/image74.png)

7.  在 **Lakehouse** 資源管理器窗格中，在 **Files** 文件夾的 ...
    菜單中，選擇 **Refres**。

![A screenshot of a computer Description automatically
generated](./media/image75.png)

8.  然後展開 **Files** 節點並驗證 是否已為表的數據文件創建
    **external_salesorder** 文件夾。

![A screenshot of a computer Description automatically
generated](./media/image76.png)

## 任務 3：比較託管表和 external tables

讓我們探討一下託管表和外部表之間的區別。

1.  在代碼單元格返回的結果下，使用 **+ Code**
    按鈕添加新的代碼單元格。將下面的代碼複製到 Code
    單元格中，然後使用單元格左側的 **▷ （*Run cell*）** 按鈕運行它。

> SqlCopy
>
> %%sql
>
> DESCRIBE FORMATTED salesorders;

![A screenshot of a computer Description automatically
generated](./media/image77.png)

![A screenshot of a computer Description automatically
generated](./media/image78.png)

2.  在結果中，查看 表的 **Location** 屬性，該屬性應該是以
    **/Tables/salesorders** 結尾的湖倉一體的 **OneLake** 存儲的路徑
    （您可能需要擴大 **Data type** 列才能查看完整路徑）。

![A screenshot of a computer Description automatically
generated](./media/image79.png)

3.  修改 **DESCRIBE** 命令以顯示 **external_saleorder**
    表的詳細信息，如下所示。

4.  在代碼單元格返回的結果下，使用 **+ Code**
    按鈕添加新的代碼單元格。複製下面的代碼，並使用單元格左側的 **▷
    （*Run cell*）** 按鈕運行它。

> SqlCopy
>
> %%sql
>
> DESCRIBE FORMATTED external_salesorder;

![A screenshot of a email Description automatically
generated](./media/image80.png)

5.  在結果中，查看 表的 **Location** 屬性，該屬性應該是以
    **/Files/external_saleorder** 結尾的湖倉一體的 OneLake 存儲路徑
    （您可能需要擴大 **Data type** 列才能查看完整路徑）。

![A screenshot of a computer Description automatically
generated](./media/image81.png)

## 任務 4：在單元格中運行 SQL 代碼

雖然能夠將 SQL 語句嵌入到包含 PySpark
代碼的單元格中很有用，但數據分析師通常只想直接在 SQL 中工作。

1.  單擊 Notebook 的 **+ Code** 單元格，然後在其中輸入以下代碼。單擊 **▷
    Run cell** 按鈕並查看結果。請注意:

    - 單元格開頭的 %%sql 行（稱為*魔術*）表示應使用 Spark SQL
      語言運行時而不是 PySpark 來運行此單元格中的代碼。

    - SQL 代碼引用 您之前創建的 **salesorders** 表。

    - SQL 查詢的輸出將自動作為結果顯示在單元格下

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

**注意**：有關 Spark SQL 和數據幀的更多信息，請參閱 [*Spark SQL
documentation*](https://spark.apache.org/docs/2.2.0/sql-programming-guide.html)。

# 練習 4：使用 Spark 可視化數據

眾所周知，一張圖片勝過千言萬語，一張圖表通常勝過一千行數據。雖然 Fabric
中的筆記本包括一個內置的圖表視圖，用於從數據幀或 Spark SQL
查詢顯示的數據，但它並不是為全面的圖表而設計的。但是，您可以使用
**matplotlib** 和 **seaborn** 等 Python
圖形庫根據數據幀中的數據創建圖表。

## 任務 1：以圖表形式查看結果

1.  單擊 Notebook 的 **+ Code** 單元格，然後在其中輸入以下代碼。單擊 **▷
    Run cell** 按鈕，並觀察它從 您之前創建的 **salesorders**
    視圖中返回數據。

> SqlCopy
>
> %%sql
>
> SELECT \* FROM salesorders

![A screenshot of a computer Description automatically
generated](./media/image84.png)

![A screenshot of a computer Description automatically
generated](./media/image85.png)

2.  在單元格下方的結果部分中，將 **View** 選項從 **Table** 更改為
    **Chart**。

![A screenshot of a computer Description automatically
generated](./media/image86.png)

3.  使用圖表右上角的 **View options**
    按鈕顯示圖表的選項窗格。然後按如下方式設置選項並選擇 **Apply** :

    - **Chart type**: Bar chart

    - **Key**: Item

    - **Values**: Quantity

    - **Series Group**: *leave blank*

    - **Aggregation**: Sum

    - **Stacked**: *Unselected*

![A blue barcode on a white background Description automatically
generated](./media/image87.png)

![A screenshot of a graph Description automatically
generated](./media/image88.png)

4.  驗證圖表是否與此類似

![A screenshot of a computer Description automatically
generated](./media/image89.png)

## 任務 2：matplotlib 入門

1.  單擊**+ Code** 並複製並粘貼以下代碼。 **Run**
    代碼並觀察它是否返回包含年收入的 Spark 數據幀。

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

2.  要將數據可視化為圖表，我們將首先使用 **matplotlib** Python
    庫。該庫是許多其他庫所基於的核心繪圖庫，並在創建圖表時提供了極大的靈活性。

3.  單擊 **+ Code**並複製並粘貼以下代碼。

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

5.  單擊 **Run cell**
    按鈕並查看結果，其中包括一個柱形圖，其中包含每年的總收入。請注意用於生成此圖表的代碼的以下功能:

    - **matplotlib** 庫需要 *Pandas* 數據幀，因此您需要將 Spark SQL
      查詢返回的 Spark 數據幀轉換為此格式。

    - **matplotlib** 庫的核心是 **pyplot**
      對象。這是大多數繪圖功能的基礎。

    - 默認設置會生成可用的圖表，但有相當大的空間可以對其進行自定義

![A screenshot of a computer screen Description automatically
generated](./media/image93.png)

6.  修改代碼以繪製圖表，如下所示，將 **Cell** 中的所有代碼替換為
    以下代碼，然後單擊 **▷ Run cell** 按鈕並查看輸出

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

7.  該圖表現在包含更多信息。從技術上講，繪圖包含在 **Figure**
    中。在前面的示例中，該圖是為您隱式創建的;但您可以顯式創建它。

8.  修改代碼以繪製圖表，如下所示，將 **cell** 中的所有代碼替換為
    以下代碼。

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

9.  **Re-run** 代碼單元並查看結果。該圖確定圖的形狀和大小。

> 一個圖窗可以包含多個子圖，每個子圖都有自己的*axis*。

![A screenshot of a computer Description automatically
generated](./media/image96.png)

10. 修改代碼以繪製圖表，如下所示。 **Re-run**
    代碼單元並查看結果。該圖窗包含代碼中指定的子圖。

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

**注意**：要瞭解有關使用 matplotlib 繪圖的更多信息，請參閱 [*matplotlib
documentation*](https://matplotlib.org/).

## 任務 3：使用 seaborn 庫

雖然 **matplotlib**
使您能夠創建多種類型的複雜圖表，但它可能需要一些複雜的代碼才能獲得最佳結果。出於這個原因，多年來，在
matplotlib
的基礎上構建了許多新的庫，以抽象其複雜性並增強其功能。**seaborn**
就是這樣一個庫。

1.  單擊 **+ Code** 並複製並粘貼以下代碼。

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

2.  **Run** 代碼並觀察它是否使用 seaborn 庫顯示條形圖。

![A screenshot of a graph Description automatically
generated](./media/image100.png)

3.  **Modify** 代碼如下。 **Run** 修改後的代碼，請注意，seaborn
    使您能夠為繪圖設置一致的顏色主題。

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

4.  再次 **Modify**代碼，如下所示。 **Run**
    修改後的代碼以折線圖的形式查看年收入。

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

**注**： 要瞭解有關使用 seaborn 進行打印的更多信息，請參見 [*seaborn
documentation*](https://seaborn.pydata.org/index.html)。

## 任務 4：使用增量 tables 流式傳輸數據

Delta Lake 支持流式處理數據。Delta 表可以是 使用 Spark Structured
Streaming API
創建的數據流的接收器或源。在此示例中，你將使用增量表作為模擬物聯網
（IoT） 場景中某些流數據的接收器。

1.  單擊 **+ Code** 並複製並粘貼以下代碼，然後單擊 **Run cell** 按鈕。

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

2.  確保消息 ***Source stream created...***
    被打印出來。您剛剛運行的代碼基於一個文件夾創建了一個流數據源，該文件夾已保存了一些數據，表示來自假設
    IoT 設備的讀數。

3.  單擊 **+ Code** 並複製並粘貼以下代碼，然後單擊運行 **Run cell**
    按鈕。

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

4.  此代碼將 delta 格式的流式處理設備數據寫入名為 **iotdevicedata**
    的文件夾。由於文件夾位置的路徑位於 **Tables**
    文件夾中，因此將自動為其創建一個表。單擊表格旁邊的水平省略號，然後單擊
    **Refresh**。

![](./media/image106.png)

![A screenshot of a computer Description automatically
generated](./media/image107.png)

5.  單擊 **+ Code** 並複製並粘貼以下代碼，然後單擊 **Run cell** 按鈕。

> SqlCopy
>
> %%sql
>
> SELECT \* FROM IotDeviceData;

![A screenshot of a computer Description automatically
generated](./media/image108.png)

6.  此代碼查詢 **IotDeviceData** 表，其中包含來自流式處理源的設備數據。

7.  單擊 **+ Code** 並複製並粘貼以下代碼，然後單擊 **Run cell** 按鈕。

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

8.  此代碼將更多假設的設備數據寫入流式處理源。

9.  單擊 **+ Code** 並複製並粘貼以下代碼，然後單擊 **Run Cell** 按鈕。

> SqlCopy
>
> %%sql
>
> SELECT \* FROM IotDeviceData;
>
> ![A screenshot of a computer Description automatically
> generated](./media/image110.png)

10. 此代碼再次查詢 **IotDeviceData**
    表，該表現在應包含已添加到流式處理源的其他數據。

11. 單擊 **+ Code** 並複製並粘貼以下代碼，然後單擊 **Run cell** 按鈕。

> CodeCopy
>
> deltastream.stop()

![A screenshot of a computer Description automatically
generated](./media/image111.png)

12. 此代碼將停止流。

## 任務 5：保存 notebook 並結束 Spark 會話

現在，您已經完成了對數據的處理，您可以使用有意義的名稱保存筆記本並結束
Spark 會話。

1.  在筆記本菜單欄中，使用 ⚙️ **Settings** 圖標查看筆記本設置。

![A screenshot of a computer Description automatically
generated](./media/image112.png)

2.  將筆記本的 **Name** 設置為 ++**Explore Sales
    Orders**++，然後關閉設置窗格。

![A screenshot of a computer Description automatically
generated](./media/image113.png)

3.  在筆記本菜單上，選擇 **Stop session** 以結束 Spark 會話。

![A screenshot of a computer Description automatically
generated](./media/image114.png)

![A screenshot of a computer Description automatically
generated](./media/image115.png)

# 練習 5：在 Microsoft Fabric 中創建數據流（第 2 代）

在 Microsoft Fabric 中，數據流 （Gen2） 連接到各種數據源，並在 Power
Query Online 中執行轉換。然後，可以在 Data Pipelines
中使用它們將數據引入 Lakehouse 或其他分析存儲，或為 Power BI
報表定義數據集。

本練習旨在介紹 Dataflows （Gen2）
的不同元素，而不是創建企業中可能存在的複雜解決方案

## 任務 1：創建 Dataflow （Gen2） 以攝取數據

現在，您有一個
Lakehouse，您需要將一些數據提取到其中。實現此目的的一種方法是定義封裝*提取、轉換和加載*
（ETL） 流程的數據流。

1.  現在，單擊 左側導航窗格中的 **Fabric_lakehouse**。

![A screenshot of a computer Description automatically
generated](./media/image116.png)

2.  在 **Fabric_lakehouse** 主頁中，單擊 **Get data** ，然後選擇 **New
    Dataflow Gen2。** 此時將打開新數據流的 Power Query 編輯器。

![](./media/image117.png)

3.  在 **Power Query** 窗格中的 **Home tab** 下，單擊 **Import from a
    Text/CSV file**。

![](./media/image118.png)

4.  在 **Connect to data source** 窗格的 **Connection settings**
    下，選擇 **Link to file （preview）**單選按鈕

- **Link to file**: *Selected*

- **File path or
  URL**: [https://raw.githubusercontent.com/MicrosoftLearning/dp-
  data/main/orders.csv](https://raw.githubusercontent.com/MicrosoftLearning/dp-%20%20data/main/orders.csv)

![](./media/image119.png)

5.  在 **Connect to data source** 窗格的 **Connection credentials**
    下，輸入以下詳細信息，然後單擊 **Next** 按鈕。

    - **Connection**: Create new connection

    - **data gateway**: (none)

    - **Authentication kind**: Organizational account

![](./media/image120.png)

6.  在 **Preview file data** 窗格中，單擊 **Create** 以創建數據源。 ![A
    screenshot of a computer Description automatically
    generated](./media/image121.png)

7.  **Power Query**
    編輯器顯示數據源和一組用於設置數據格式的初始查詢步驟。

![A screenshot of a computer Description automatically
generated](./media/image122.png)

8.  在工具欄功能區上，選擇 **Add column** 選項卡。然後，選擇 **Custom
    column。**

![A screenshot of a computer Description automatically
generated](./media/image123.png) 

9.  將 新列名稱 設置為 **MonthNo** ，將 數據類型 設置為 Whole
    Number，然後在 **Custom column formula** 下
    添加以下公式：**Date.Month(\[OrderDate\])** 。選擇 **OK。**

![A screenshot of a computer Description automatically
generated](./media/image124.png)

10. 請注意添加自定義列的步驟是如何添加到查詢中的。結果列將顯示在數據窗格中。

![A screenshot of a computer Description automatically
generated](./media/image125.png)

**提示：**在右側的 Query Settings 窗格中，請注意 **Applied Steps**
包括每個轉換步驟。在底部，您還可以切換 **Diagram flow** 按鈕以打開步驟的
Visual Diagram（可視化圖表）。

可以向上或向下移動步驟，通過選擇齒輪圖標進行編輯，並且您可以選擇每個步驟以在預覽窗格中查看應用的轉換。

任務 2：為 Dataflow 添加數據目標

1.  在 **Power Query** 工具欄功能區上，選擇 **Home** 選項卡。然後，在
    Data destination 下拉菜單中，選擇 **Lakehouse**（如果尚未選擇）。

![](./media/image126.png)

![A screenshot of a computer Description automatically
generated](./media/image127.png)

**注意：**如果此選項灰顯，則您可能已經設置了數據目標。檢查 Power Query
編輯器右側 Query settings （查詢設置）
窗格底部的數據目標。如果已設置目標，則可以使用 齒輪 更改它。

2.  單擊 所選 **Lakehouse** 選項旁邊的 **Settings** 圖標。

![A screenshot of a computer Description automatically
generated](./media/image128.png)

3.  在 **Connect to data destination** 對話框中，選擇 **Edit
    connection。**

![A screenshot of a computer Description automatically
generated](./media/image129.png)

4.  在 **Connect to data destination** 對話框中，選擇 **Sign in**
    使用您的 Power BI 組織帳戶登錄 ,
    以設置數據流用於訪問湖倉一體的身份。

![A screenshot of a computer Description automatically
generated](./media/image130.png)

![A screenshot of a computer Description automatically
generated](./media/image131.png)

5.  在 Connect to data destination 對話框中，選擇 **Next**

![A screenshot of a computer Description automatically
generated](./media/image132.png)

6.  在 **Connect to data destination** 對話框中，選擇 **New
    table**。單擊 **Lakehouse folder** ，選擇您的工作區 –
    **dp_FabricXX** 然後選擇您的 Lakehouse，即 **Fabric_lakehouse。**
    然後將 Table name 指定為 **orders** 並選擇 **Next** 按鈕。

![A screenshot of a computer Description automatically
generated](./media/image133.png)

7.  在 **Choose destination settings** 對話框的 **Use automatic settings
    off** 和 **Update method** 下，選擇 **Append** ，然後單擊 **Save
    settings** 按鈕。

![](./media/image134.png)

8.  **Lakehouse** 目標在 **Power Query** 編輯器的 **query** 中指示為
    **icon。**

![A screenshot of a computer Description automatically
generated](./media/image135.png)

![A screenshot of a computer Description automatically
generated](./media/image136.png)

9.  選擇 **Publish ** 以發佈數據流。然後等待 **Dataflow 1**
    數據流在您的工作區中創建。

![A screenshot of a computer Description automatically
generated](./media/image137.png)

10. 發佈後，您可以右鍵單擊工作區中的數據流，選擇
    **Properties**，然後重命名數據流。

![A screenshot of a computer Description automatically
generated](./media/image138.png)

11. 在 **Dataflow1** 對話框中，輸入 **Name** 作為 **Gen2_Dataflow**
    然後單擊 **Save** 按鈕。

![A screenshot of a computer Description automatically
generated](./media/image139.png)

![](./media/image140.png)

## 任務 3：將數據流添加到管道

您可以將數據流作為活動包含在管道中。管道用於編排數據攝取和處理活動，使您能夠在單個計劃流程中將數據流與其他類型的作相結合。可以在幾種不同的體驗中創建管道，包括數據工廠體驗。

1.  在 Synapse 數據工程主頁的 **dp_FabricXX** 窗格下，選擇 **+New -\>
    Data pipeline**

![](./media/image141.png)

2.  在 **New pipeline** 對話框中，在 **Name** 字段中輸入 **Load data**
    ，然後單擊 **Create** 按鈕以打開新管道。

![A screenshot of a computer Description automatically
generated](./media/image142.png)

3.  管道編輯器隨即打開。

![A screenshot of a computer Description automatically
generated](./media/image143.png)

> **提示**： 如果 Copy Data 嚮導自動打開，請將其關閉！

4.  選擇 **Pipeline activity，**然後向管道中添加 **Dataflow** 活動。

![](./media/image144.png)

5.  選擇新的 **Dataflow1** 活動後，在 **Settings** 選項卡的 **Dataflow**
    下拉列表中，選擇 **Gen2_Dataflow** （您之前創建的數據流）

![](./media/image145.png)

6.  在 **Home** 選項卡上，使用 **🖫 （*Save*）** 圖標保存管道。

![A screenshot of a computer Description automatically
generated](./media/image146.png)

7.  使用 **▷ Run** 按鈕運行管道，並等待其完成。這可能需要幾分鐘時間。

> ![A screenshot of a computer Description automatically
> generated](./media/image147.png)
>
> ![A screenshot of a computer Description automatically
> generated](./media/image148.png)

![A screenshot of a computer Description automatically
generated](./media/image149.png)

8.  在左邊緣的菜單欄中，選擇您的工作區，即 **dp_FabricXX**。

![A screenshot of a computer Description automatically
generated](./media/image150.png)

9.  在 **Fabric_lakehouse** 窗格中，選擇 Lakehouse
    類型的**Gen2_FabricLakehouse**。

![A screenshot of a computer Description automatically
generated](./media/image151.png)

12. 在 **Explorer** 窗格中，選擇 **...** 菜單，選擇
    **Refresh**。然後展開 **Tables** 並選擇 **orders** 表，該表已由
    **dataflow**。

![A screenshot of a computer Description automatically
generated](./media/image152.png)

![](./media/image153.png)

**提示**： 使用 Power BI Desktop
*數據流連接器*直接連接到通過數據流完成的數據轉換。

您還可以進行其他轉換，發佈為新數據集，並與專用數據集的目標受眾一起分發。

## 任務 4：清理資源

在本練習中，您學習了如何使用 Spark 處理 Microsoft Fabric 中的數據。

如果您已完成對 Lakehouse 的探索，則可以刪除為本練習創建的工作區。

1.  在左側的欄中，選擇工作區的圖標以查看其包含的所有項目。

> ![A screenshot of a computer Description automatically
> generated](./media/image154.png)

2.  在 **...** 菜單中，選擇 **Workspace settings**。

![](./media/image155.png)

3.  選擇 **General** 並單擊 **Remove this workspace。**

![A screenshot of a computer settings Description automatically
generated](./media/image156.png)

4.  在 **Delete workspace?** 對話框中，單擊 **Delete** 按鈕。

> ![A screenshot of a computer Description automatically
> generated](./media/image157.png)
>
> ![A screenshot of a computer Description automatically
> generated](./media/image158.png)

**總結**

此 用例 將指導您完成在 Power BI 中使用 Microsoft Fabric
的過程。它涵蓋各種任務，包括設置工作區、創建湖倉一體、上傳和管理數據文件以及使用
Notebook 進行數據探索。參與者將學習如何使用
PySpark作和轉換數據、創建可視化以及保存和分區數據以實現高效查詢。

在此用例中，參與者將參與一系列任務，重點是在 Microsoft Fabric
中使用增量表。這些任務包括上傳和探索數據、創建託管和外部增量表、比較它們的屬性，該實驗室介紹了用於管理結構化數據的
SQL 功能，並使用 matplotlib 和 seaborn 等 Python
庫提供有關數據可視化的見解。這些練習旨在全面瞭解如何利用 Microsoft
Fabric 進行數據分析，以及在 IoT 上下文中合併增量表以流式傳輸數據。

此使用案例將指導您完成設置 Fabric
工作區、創建數據湖倉一體和攝取數據進行分析的過程。它演示了如何定義數據流來處理
ETL作並配置數據目標以存儲轉換後的數據。此外，您還將學習如何將數據流集成到管道中以進行自動化處理。最後，您將獲得在練習完成後清理資源的說明。

此實驗室為您提供使用 Fabric
的基本技能，使您能夠創建和管理工作區、建立數據湖倉一體以及高效執行數據轉換。通過將數據流整合到管道中，您將學習如何自動執行數據處理任務、簡化工作流程並提高實際場景中的生產力。清理說明可確保您不會留下不必要的資源，從而促進有序且高效的工作區管理方法。
