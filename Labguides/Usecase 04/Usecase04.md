# Anwendungsfall 04: Moderne Analysen im Cloud-Maßstab mit Azure Databricks und Microsoft Fabric

**Einleitung**

In diesem Lab untersuchen Sie die Integration von Azure Databricks mit
Microsoft Fabric, um ein Lakehouse mithilfe der Medallion-Architektur zu
erstellen und zu verwalten, eine Delta-Tabelle mithilfe Ihres Azure Data
Lake Storage (ADLS) Gen2-Kontos mit Azure Databricks zu erstellen und
Daten mit Azure Databricks zu erfassen. Dieser praktische Leitfaden
führt Sie durch die Schritte, die erforderlich sind, um ein Lakehouse zu
erstellen, Daten darin zu laden und die strukturierten Datenschichten zu
erkunden, um eine effiziente Datenanalyse und Berichterstellung zu
ermöglichen.

Die Medaillon-Architektur besteht aus drei unterschiedlichen Schichten
(oder Zonen).

- Bronze: Diese erste Ebene, die auch als Rohzone bezeichnet wird,
  speichert Quelldaten in ihrem ursprünglichen Format. Die Daten in
  diesem Layer sind in der Regel nur durch Anfügen versehen und
  unveränderlich.

- Silber: Diese Schicht, die auch als angereicherte Zone bezeichnet
  wird, speichert Daten, die aus der Bronzeschicht stammen. Die Rohdaten
  wurden bereinigt und standardisiert und sind nun als Tabellen (Zeilen
  und Spalten) strukturiert. Es kann auch in andere Daten integriert
  werden, um eine Unternehmensansicht aller Geschäftseinheiten wie
  Kunde, Produkt und andere bereitzustellen.

- Gold: Diese letzte Schicht, die auch als kuratierte Zone bezeichnet
  wird, speichert Daten, die aus der Silberschicht stammen. Die Daten
  werden verfeinert, um spezifische nachgelagerte Geschäfts- und
  Analyseanforderungen zu erfüllen. Tabellen entsprechen in der Regel
  dem Sternschemadesign, das die Entwicklung von Datenmodellen
  unterstützt, die für Leistung und Benutzerfreundlichkeit optimiert
  sind.

**Ziele**:

- Machen Sie sich mit den Prinzipien der Medallion-Architektur in
  Microsoft Fabric Lakehouse vertraut.

- Implementieren Sie einen strukturierten Datenmanagementprozess mit
  Medallion-Schichten (Bronze, Silber, Gold).

- Wandeln Sie Rohdaten in validierte und angereicherte Daten für
  erweiterte Analysen und Berichte um.

- Lernen Sie Best Practices für Datensicherheit, CI/CD und effiziente
  Datenabfragen kennen.

- Laden Sie Daten mit dem OneLake-Datei-Explorer in OneLake hoch.

- Verwenden Sie ein Fabric-Notizbuch, um Daten in OneLake zu lesen und
  als Delta-Tabelle zurückzuschreiben.

- Analysieren und transformieren Sie Daten mit Spark mithilfe eines
  Fabric-Notebooks.

- Abfragen einer Kopie der Daten in OneLake mit SQL.

- Erstellen Sie mit Azure Databricks eine Delta-Tabelle in Ihrem Azure
  Data Lake Storage (ADLS) Gen2-Konto.

- Erstellen Sie eine OneLake-Verknüpfung zu einer Delta-Tabelle in ADLS.

- Verwenden Sie Power BI, um Daten über die ADLS-Verknüpfung zu
  analysieren.

- Lesen und Ändern einer Delta-Tabelle in OneLake mit Azure Databricks.

# Übung 1: Importieren von Beispieldaten in Lakehouse

In dieser Übung durchlaufen Sie den Prozess des Erstellens eines
Lakehouse und des Ladens von Daten in dieses Haus mithilfe von Microsoft
Fabric.

Aufgabe:Weg

## **Aufgabe 1: Erstellen eines Fabric-Workspace**

In dieser Aufgabe erstellen Sie einen Fabric-Workspace. Der Workspace
enthält alle Elemente, die für dieses Lakehouse-Tutorial erforderlich
sind, einschließlich Lakehouse, Dataflows, Data Factory-Pipelines,
Notebooks, Power BI-Datasets und Berichte.

1.  Öffnen Sie Ihren Browser, navigieren Sie zur Adressleiste, und geben
    Sie die folgende URL ein oder fügen Sie sie ein:
    [https://app.fabric.microsoft.com/](https://app.fabric.microsoft.com/,)
    Drücken Sie dann **Enter**.

> ![A search engine window with a red box Description automatically
> generated with medium confidence](./media/image1.png)

2.  Wechseln Sie zurück zum **Power** BI-Fenster. Navigieren Sie im
    linken Navigationsmenü der Power BI-Startseite zu und klicken Sie
    auf **Workspaces**.

![](./media/image2.png)

3.  Klicken Sie im Bereich Workspace auf **+** **New workspace.**

> ![](./media/image3.png)

4.  Geben Sie im Bereich **Create a workspace**, der auf der rechten
    Seite angezeigt wird, die folgenden Details ein, und klicken Sie auf
    die Schaltfläche **Apply**.

[TABLE]

> ![](./media/image4.png)

![A screenshot of a computer Description automatically
generated](./media/image5.png)

5.  Warten Sie, bis die Bereitstellung abgeschlossen ist. Es dauert 2-3
    Minuten, bis der Vorgang abgeschlossen ist.

![](./media/image6.png)

## **Aufgabe 2: Erstellen Sie ein Lakehouse**

1.  Klicken Sie auf der Seite **Power BI Fabric Lakehouse Tutorial-XX**
    auf das Power **BI-Symbol** unten links, und wählen Sie **Data
    Engineering** aus.

> ![](./media/image7.png)

2.  Wählen Sie auf der **Synapse** **Data Engineering** **Home** die
    Option **Lakehouse** aus, um ein Lakehouse zu erstellen.

![](./media/image8.png)

![A screenshot of a computer Description automatically
generated](./media/image9.png)

3.  Geben Sie im Dialogfeld **New Lakehouse** im Feld **Name** den Namen
    **wwilakehouse** ein, klicken Sie auf die Schaltfläche **Create**
    und öffnen Sie das neue Lakehouse.

> **Hinweis**: Stellen Sie sicher, dass Sie vor **wwilakehouse**
> Leerzeichen entfernen.
>
> ![](./media/image10.png)
>
> ![A screenshot of a computer Description automatically
> generated](./media/image11.png)
>
> ![](./media/image12.png)

4.  Sie sehen eine Benachrichtigung mit dem Hinweis **Successfully
    created SQL endpoint**.

> ![](./media/image13.png)

# Übung 2: Implementieren der Medallion-Architektur mit Azure Databricks

## **Aufgabe 1: Einrichten der Bronzeschicht**

1.  Wählen Sie auf der Seite **wwilakehouse** das Symbol Mehr neben den
    Dateien (...) und dann **New Subfolder** aus**.**

![](./media/image14.png)

2.  Geben Sie im Popup-Fenster den Ordnernamen als **Bronze** ein, und
    wählen Sie **Create**.

![A screenshot of a computer Description automatically
generated](./media/image15.png)

3.  Wählen Sie nun das Symbol More neben den Bronze-Dateien (...) aus,
    wählen Sie **Upload** und dann, **upload files**.

![A screenshot of a computer Description automatically
generated](./media/image16.png)

4.  Wählen Sie im Bereich **upload file** das Optionsfeld **Upload
    file** aus. Klicken Sie auf die Schaltfläche **browse** und
    navigieren Sie zu **C:\LabFiles**, wählen Sie dann die erforderliche
    Datei mit Verkaufsdatendateien (2019, 2020, 2021) aus und klicken
    Sie auf die Schaltfläche **Open**.

Wählen Sie dann **Upload** aus, um die Dateien in den neuen Ordner
"Bronze" in Ihrem Lakehouse hochzuladen.

![A screenshot of a computer Description automatically
generated](./media/image17.png)

> ![A screenshot of a computer Description automatically
> generated](./media/image18.png)

5.  Klicken Sie auf den **Bronze**-Ordner, um zu bestätigen, dass die
    Dateien erfolgreich hochgeladen wurden und die Dateien.

![A screenshot of a computer Description automatically
generated](./media/image19.png)

# Übung 3: Transformieren von Daten mit Apache Spark und Abfragen mit SQL in der Medallion-Architektur

## **Aufgabe 1: Transformieren von Daten und Laden in die silberne Delta-Tabelle**

Navigieren Sie auf der Seite **wwilakehouse** und klicken Sie in der
Befehlsleiste auf **Open Notebook** und wählen Sie dann **New Notebook**
aus.

![A screenshot of a computer Description automatically
generated](./media/image20.png)

1.  Wählen Sie die erste Zelle aus (die derzeit eine *Codezelle ist*)
    und verwenden Sie dann in der dynamischen Symbolleiste oben rechts
    die Schaltfläche **M↓**, um **die Zelle in eine Markdown-Zelle
    umzuwandeln**.

![A screenshot of a computer Description automatically
generated](./media/image21.png)

2.  Wenn sich die Zelle in eine Markdown-Zelle ändert, wird der darin
    enthaltene Text gerendert.

![A screenshot of a computer Description automatically
generated](./media/image22.png)

3.  Verwenden Sie die **🖉** Schaltfläche (Bearbeiten), um die Zelle in
    den Bearbeitungsmodus zu versetzen, ersetzen Sie den gesamten Text
    und ändern Sie dann das Markdown wie folgt:

CodeCopy

\# Sales order data exploration

Verwenden Sie den Code in diesem Notebook, um Auftragsdaten zu
untersuchen.

![A screenshot of a computer Description automatically
generated](./media/image23.png)

![A screenshot of a computer Description automatically
generated](./media/image24.png)

4.  Klicken Sie auf eine beliebige Stelle im Notebook außerhalb der
    Zelle, um die Bearbeitung zu beenden und das gerenderte Markdown
    anzuzeigen.

![A screenshot of a computer Description automatically
generated](./media/image25.png)

5.  Verwenden Sie das Symbol **+** Code unter der Zellenausgabe, um dem
    Notebook eine neue Codezelle hinzuzufügen.

![A screenshot of a computer Description automatically
generated](./media/image26.png)

6.  Verwenden Sie nun das Notebook, um die Daten aus der Bronze-Schicht
    in einen Spark-DataFrame zu laden.

Wählen Sie die vorhandene Zelle im Notebook aus, die einen einfachen
auskommentierten Code enthält. Markieren und löschen Sie diese beiden
Zeilen - Sie benötigen diesen Code nicht.

*Hinweis: Mit Notebooks können Sie Code in einer Vielzahl von Sprachen
ausführen, einschließlich Python, Scala und SQL. In dieser Übung
verwenden Sie PySpark und SQL. Sie können auch Markdownzellen
hinzufügen, um formatierten Text und Bilder zum Dokumentieren des Codes
bereitzustellen.*

Geben Sie dazu den folgenden Code ein und klicken Sie auf **Run**.

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

***Hinweis**: Da dies das erste Mal ist, dass Sie Spark-Code in diesem
Notebook ausführen, muss eine Spark-Sitzung gestartet werden. Das
bedeutet, dass die erste Ausführung etwa eine Minute dauern kann.
Nachfolgende Durchläufe sind schneller.*

![A screenshot of a computer Description automatically
generated](./media/image28.png)

7.  Der Code, den Sie ausgeführt haben, lud die Daten aus den
    CSV-Dateien im **Bronze**-Ordner in einen Spark-Datenrahmen und
    zeigte dann die ersten Zeilen des Datenrahmens an.

> **Hinweis**: Sie können den Inhalt der Zellenausgabe löschen,
> ausblenden und automatisch in der Größe ändern, indem Sie die
> Schaltfläche **...** Menü oben links im Ausgabebereich.

8.  Jetzt fügen Sie **Spalten für die Datenüberprüfung und -bereinigung
    hinzu**, indem Sie einen PySpark-Datenrahmen verwenden, um Spalten
    hinzuzufügen und die Werte einiger der vorhandenen Spalten zu
    aktualisieren. Verwenden Sie die Schaltfläche +, um **einen neuen
    Codeblock hinzuzufügen**, und fügen Sie der Zelle den folgenden Code
    hinzu:

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
> Die erste Zeile des Codes importiert die erforderlichen Funktionen aus
> PySpark. Anschließend fügen Sie dem Datenrahmen neue Spalten hinzu,
> damit Sie den Namen der Quelldatei nachverfolgen können, ob der
> Auftrag als vor dem Geschäftsjahr von Interesse gekennzeichnet wurde
> und wann die Zeile erstellt und geändert wurde.
>
> Schließlich aktualisieren Sie die Spalte "CustomerName" auf "Unknown",
> wenn sie null oder leer ist.
>
> Führen Sie dann die Zelle aus, um den Code mit der Schaltfläche
> **\*\*▷** (*Run Cell*)\*\* auszuführen.

![A screenshot of a computer Description automatically
generated](./media/image29.png)

![A screenshot of a computer Description automatically
generated](./media/image30.png)

9.  Als Nächstes definieren Sie das Schema für die Tabelle
    **sales_silver** in der sales-Datenbank im Delta Lake-Format.
    Erstellen Sie einen neuen Codeblock, und fügen Sie der Zelle den
    folgenden Code hinzu:

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

   

10. Führen Sie die Zelle aus, um den Code mit der Schaltfläche **\*\*▷**
    (*Run Cell*)\*\* auszuführen.

11. Wählen Sie die Option **...** im Abschnitt Tabellen des
    Lakehouse-Explorer-Bereichs, und wählen Sie **Refresh** aus. Die
    neue **sales_silver** Tabelle sollte nun aufgelistet sein. Das **▲**
    (Dreieckssymbol) zeigt an, dass es sich um eine Delta-Tabelle
    handelt.

> **Hinweis**: Wenn die neue Tabelle nicht angezeigt wird, warten Sie
> einige Sekunden, und wählen Sie dann erneut **Refresh** aus, oder
> aktualisieren Sie die gesamte Browserregisterkarte.
>
> ![A screenshot of a computer Description automatically
> generated](./media/image31.png)
>
> ![A screenshot of a computer Description automatically
> generated](./media/image32.png)

12. Jetzt führen Sie einen **upsert operation** für eine Delta-Tabelle
    aus, aktualisieren vorhandene Datensätze basierend auf bestimmten
    Bedingungen und fügen neue Datensätze ein, wenn keine
    Übereinstimmung gefunden wird. Fügen Sie einen neuen Codeblock
    hinzu, und fügen Sie den folgenden Code ein:

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

13. Führen Sie die Zelle aus, um den Code mit der Schaltfläche **\*\*▷**
    (***Run Cell***) \*\* auszuführen.

![A screenshot of a computer Description automatically
generated](./media/image33.png)

Dieser Vorgang ist wichtig, da er es Ihnen ermöglicht, vorhandene
Datensätze in der Tabelle basierend auf den Werten bestimmter Spalten zu
aktualisieren und neue Datensätze einzufügen, wenn keine Übereinstimmung
gefunden wird. Dies ist eine häufige Anforderung, wenn Sie Daten aus
einem Quellsystem laden, das Aktualisierungen vorhandener und neuer
Datensätze enthalten kann.

Sie verfügen nun über Daten in Ihrer Silver-Delta-Tabelle, die für die
weitere Transformation und Modellierung bereit sind.

Sie haben erfolgreich Daten aus Ihrer Bronze-Schicht übernommen,
transformiert und in eine silberne Delta-Tabelle geladen. Jetzt
verwenden Sie ein neues Notebook, um die Daten weiter zu transformieren,
sie in ein Sternschema zu modellieren und in goldene Delta-Tabellen zu
laden.

*Beachten Sie, dass Sie all dies in einem einzigen Notebook hätten tun
können, aber für die Zwecke dieser Übung verwenden Sie separate
Notebooks, um den Prozess der Transformation von Daten von Bronze zu
Silber und dann von Silber zu Gold zu veranschaulichen. Dies kann beim
Debuggen, bei der Fehlerbehebung und bei der Wiederverwendung hilfreich
sein*.

## **Aufgabe 2: Laden von Daten in Gold-Delta-Tabellen**

1.  Zurück zur Startseite des Fabric Lakehouse Tutorial-29.

> ![A screenshot of a computer Description automatically
> generated](./media/image34.png)

2.  Wählen Sie **wwilakehouse** aus**.**

![A screenshot of a computer Description automatically
generated](./media/image35.png)

3.  Im Explorer-Bereich "Lakehouse" sollte die Tabelle
    **"sales_silver"** im Abschnitt **Tables** des Explorer-Bereichs
    aufgeführt sein.

![A screenshot of a computer Description automatically
generated](./media/image36.png)

4.  Erstellen Sie nun ein neues Notebook mit dem Namen **Transform data
    for Gold**. Navigieren Sie dazu und klicken Sie in der Befehlsleiste
    auf **Open Notebook** und wählen Sie dann **New Notebook** aus.

![A screenshot of a computer Description automatically
generated](./media/image37.png)

5.  Entfernen Sie im vorhandenen Codeblock den Textbaustein, und **fügen
    Sie den folgenden Code hinzu,** um Daten in Ihren Datenrahmen zu
    laden, mit dem Erstellen Ihres Sternschemas zu beginnen und es dann
    auszuführen:

> CodeCopy

\# Load data to the dataframe as a starting point to create the gold
layer

df = spark.read.table("wwilakehouse.sales_silver")

\# Display the first few rows of the dataframe to verify the data

df.show()

![A screenshot of a computer Description automatically
generated](./media/image38.png)

6.  **Fügen Sie als Nächstes einen neuen Codeblock hinzu,** und fügen
    Sie den folgenden Code ein, um Ihre Datumsdimensionstabelle zu
    erstellen und auszuführen:

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

**Hinweis**: Sie können den Befehl display(df) jederzeit ausführen, um
den Fortschritt Ihrer Arbeit zu überprüfen. In diesem Fall würden Sie
'display(dfdimDate_gold)" ausführen, um den Inhalt des dimDate_gold
Datenrahmens anzuzeigen.

7.  Fügen Sie in einem neuen Codeblock **den folgenden Code hinzu, und
    führen Sie ihn aus**, um einen Datenrahmen für die Datumsdimension
    zu erstellen, **dimdate_gold**:

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

8.  Sie unterteilen den Code in neue Codeblöcke, damit Sie verstehen und
    beobachten können, was im Notebook geschieht, während Sie die Daten
    transformieren. Fügen Sie in einem anderen neuen Codeblock **den
    folgenden Code hinzu, und führen Sie ihn aus**, um die
    Datumsdimension zu aktualisieren, wenn neue Daten eingehen:

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

> Ihre Datumsdimension ist eingerichtet.

![A screenshot of a computer Description automatically
generated](./media/image43.png)

## **Aufgabe 3: Erstellen der Kundendimension.**

1.  Um die Kundendimensionstabelle zu erstellen, **fügen Sie einen neuen
    Codeblock hinzu**, fügen Sie den folgenden Code ein, und führen Sie
    ihn aus:

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

2.  Fügen Sie in einem neuen Codeblock **den folgenden Code hinzu, und
    führen Sie ihn aus**, um doppelte Kunden zu löschen, bestimmte
    Spalten auszuwählen und die Spalte "CustomerName" zu teilen, um die
    Spalten "First " und "Last" zu erstellen:

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

Hier haben Sie einen neuen DataFrame-dfdimCustomer_silver erstellt,
indem Sie verschiedene Transformationen durchgeführt haben, z. B. das
Löschen von Duplikaten, das Auswählen bestimmter Spalten und das Teilen
der Spalte "CustomerName", um die Spalten "First" und "Last" zu
erstellen. Das Ergebnis ist ein DataFrame mit bereinigten und
strukturierten Kundendaten, einschließlich separater Spalten "First" und
"Last", die aus der Spalte "Kundenname" extrahiert wurden.

![A screenshot of a computer Description automatically
generated](./media/image47.png)

3.  Als Nächstes erstellen wir **die ID-Spalte für unsere Kunden**.
    Fügen Sie in einem neuen Codeblock Folgendes ein, und führen Sie es
    aus:

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

4.  Jetzt stellen Sie sicher, dass Ihre Kundentabelle auf dem neuesten
    Stand bleibt, wenn neue Daten eingehen. **Fügen Sie in einem neuen
    Codeblock** Folgendes ein, und führen Sie es aus:

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

5.  Jetzt wiederholen Sie **diese Schritte, um Ihre Produktdimension zu
    erstellen**. Fügen Sie in einem neuen Codeblock Folgendes ein, und
    führen Sie es aus:

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

6.  **Fügen Sie einen weiteren Codeblock** hinzu, um den
    **product_silver** Datenrahmen zu erstellen.

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

7.  Jetzt erstellen Sie IDs für Ihre **dimProduct_gold Tabelle**. Fügen
    Sie einem neuen Codeblock die folgende Syntax hinzu, und führen Sie
    sie aus:

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

Dadurch wird die nächste verfügbare Produkt-ID basierend auf den
aktuellen Daten in der Tabelle berechnet, diese neuen IDs den Produkten
zugewiesen und dann die aktualisierten Produktinformationen angezeigt.

![A screenshot of a computer Description automatically
generated](./media/image57.png)

8.  Ähnlich wie bei Ihren anderen Dimensionen müssen Sie sicherstellen,
    dass Ihre Produkttabelle auf dem neuesten Stand bleibt, wenn neue
    Daten eingehen. **Fügen Sie in einem neuen Codeblock** Folgendes
    ein, und führen Sie es aus:

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

**Nachdem Sie nun Ihre Dimensionen erstellt haben, besteht der letzte
Schritt darin, die Faktentabelle zu erstellen.**

9.  **Fügen Sie in einem neuen Codeblock** den folgenden Code ein, und
    führen Sie ihn aus, um die **Faktentabelle zu erstellen**:

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

10. **Fügen Sie in einem neuen Codeblock** den folgenden Code ein, und
    führen Sie ihn aus, um einen **neuen Datenrahmen** zu erstellen, um
    Verkaufsdaten mit Kunden- und Produktinformationen zu kombinieren,
    einschließlich Kunden-ID, Artikel-ID, Bestelldatum, Menge,
    Stückpreis und Steuer:

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

1.  Jetzt stellen Sie sicher, dass die Verkaufsdaten auf dem neuesten
    Stand bleiben, indem Sie den folgenden Code in einem **neuen
    Codeblock ausführen**:

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

Hier verwenden Sie den Zusammenführungsvorgang von Delta Lake, um die
factsales_gold Tabelle mit neuen Umsatzdaten (dffactSales_gold) zu
synchronisieren und zu aktualisieren. Der Vorgang vergleicht das
Bestelldatum, die Kunden-ID und die Artikel-ID zwischen den vorhandenen
Daten (silberne Tabelle) und den neuen Daten (aktualisierter DataFrame),
aktualisiert übereinstimmende Datensätze und fügt bei Bedarf neue
Datensätze ein.

![A screenshot of a computer Description automatically
generated](./media/image66.png)

Sie verfügen nun über eine modeled **gold** layer, die für Berichte und
Analysen verwendet werden kann.

# Übung 4: Einrichten der Konnektivität zwischen Azure Databricks und Azure Data Lake Storage (ADLS) Gen 2

Erstellen Sie nun mithilfe Ihres Azure Data Lake Storage (ADLS)
Gen2-Kontos mithilfe von Azure Databricks eine Delta-Tabelle.
Anschließend erstellen Sie eine OneLake-Verknüpfung zu einer
Delta-Tabelle in ADLS und verwenden Power BI, um Daten über die
ADLS-Verknüpfung zu analysieren.

## **Aufgabe 0: Einlösen einer Azure-Karte und Aktivieren des Azure-Abonnements**

1.  Navigieren Sie unter folgendem Link !!
    https://www.microsoftazurepass.com/!! und klicken Sie auf die
    Schaltfläche **Start**.

![](./media/image67.png)

2.  Geben Sie auf der Microsoft-Anmeldeseite die **Tenant-ID** ein**,**
    klicken Sie auf **Next**.

![](./media/image68.png)

3.  Geben Sie auf der nächsten Seite Ihr Passwort ein und klicken Sie
    auf **Sign In**.

![](./media/image69.png)

![A screenshot of a computer error Description automatically
generated](./media/image70.png)

4.  Sobald Sie angemeldet sind, klicken Sie auf der Microsoft
    Azure-Seite auf die Registerkarte **Confirm Microsoft Account**.

![](./media/image71.png)

5.  Geben Sie auf der nächsten Seite den Promo-Code und die
    Captcha-Zeichen ein und klicken Sie auf **Submit.**

![A screenshot of a computer Description automatically
generated](./media/image72.png)

![A screenshot of a computer error Description automatically
generated](./media/image73.png)

6.  Geben Sie auf der Seite Ihr Profil Ihre Profildaten ein und klicken
    Sie auf **Sign up.**

7.  Wenn Sie dazu aufgefordert werden, registrieren Sie sich für die
    Multifaktor-Authentifizierung, und melden Sie sich dann beim
    Azure-Portal an, indem Sie zum folgenden Link navigieren!!
    <https://portal.azure.com/#home>!!

![](./media/image74.png)

8.  Geben Sie in der Suchleiste Abonnement ein und klicken Sie auf das
    Symbol Abonnement unter **Services.**

![A screenshot of a computer Description automatically
generated](./media/image75.png)

9.  Nach erfolgreicher Einlösung von Azure Pass wird eine Abonnement-ID
    generiert.

![](./media/image76.png)

## **Aufgabe 1: Erstellen eines Azure Data Storage-Kontos**

1.  Melden Sie sich mit Ihren Azure-Anmeldeinformationen bei Ihrem
    Azure-Portal an.

2.  Wählen Sie auf der Startseite im Menü des linken Portals die Option
    **Storage Account** aus, um eine Liste Ihrer Speicherkonten
    anzuzeigen. Wenn das Portalmenü nicht angezeigt wird, wählen Sie die
    Menüschaltfläche aus, um es zu aktivieren.

![A screenshot of a computer Description automatically
generated](./media/image77.png)

3.  Wählen Sie auf der Seite **Storage Account** die Option **Create**.

![A screenshot of a computer Description automatically
generated](./media/image78.png)

4.  Geben Sie auf der Registerkarte Grundlagen nach Auswahl einer
    Ressourcengruppe die wesentlichen Informationen für Ihr
    Speicherkonto an:

[TABLE]

Lassen Sie die anderen Einstellungen unverändert und wählen Sie
**Review + Create** aus, um die Standardoptionen zu übernehmen und mit
der Validierung und Erstellung des Kontos fortzufahren.

Hinweis: Wenn Sie noch keine Ressourcengruppe erstellt haben, können Sie
auf "**Create New**" klicken und eine neue Ressource für Ihr
Speicherkonto erstellen.

![](./media/image79.png)

![](./media/image80.png)

![A screenshot of a computer Description automatically
generated](./media/image81.png)

![A screenshot of a computer Description automatically
generated](./media/image82.png)

![A screenshot of a computer Description automatically
generated](./media/image83.png)

5.  Wenn Sie zur Registerkarte **Review + create** navigieren, führt
    Azure die Überprüfung für die von Ihnen ausgewählten
    Speicherkontoeinstellungen aus. Wenn die Überprüfung erfolgreich
    ist, können Sie mit dem Erstellen des Speicherkontos fortfahren.

Wenn die Validierung fehlschlägt, gibt das Portal an, welche
Einstellungen geändert werden müssen.

![A screenshot of a computer Description automatically
generated](./media/image84.png)

![A screenshot of a computer Description automatically
generated](./media/image85.png)

Sie haben jetzt Ihr Azure-Datenspeicherkonto erfolgreich erstellt.

6.  Navigieren Sie zur Seite Storage Account indem Sie in der Suchleiste
    oben auf der Seite suchen, und wählen Sie das neu erstellte
    Speicherkonto aus.

![A screenshot of a computer Description automatically
generated](./media/image86.png)

7.  Navigieren Sie auf der Seite des Speicherkontos zu **Container**
    unter **Data storage,** und erstellen Sie im linken
    Navigationsbereich einen neuen Container mit dem Namen!!
    Medaillon1!! und klicken Sie auf die Schaltfläche **Create**. 

 

![A screenshot of a computer Description automatically
generated](./media/image87.png)

8.  Navigieren Sie nun zurück auf der **storage account**, und wählen
    Sie im linken Navigationsmenü **Endpoints** aus. Scrollen Sie nach
    unten, kopieren Sie die URL des **Primary endpoint URL**, und fügen
    Sie sie in einem Editor ein. Dies ist hilfreich beim Erstellen der
    Verknüpfung.

![](./media/image88.png)

9.  Navigieren Sie auf ähnliche Weise zu den **Access keys** im selben
    Navigationsbereich.

![A screenshot of a computer Description automatically
generated](./media/image89.png)

## **Aufgabe 2: Erstellen einer Delta-Tabelle, Erstellen einer Verknüpfung und Analysieren der Daten in Ihrem Lakehouse**

1.  Wählen Sie in Ihrem Lakehouse die Ellipsen **aus (...)** neben
    Dateien und wählen Sie dann **New shortcut**.

![](./media/image90.png)

2.  Wählen Sie auf dem Bildschirm **New Shortcut** die Kachel **Azure
    Data Lake Storage Gen2** aus.

![Screenshot of the tile options in the New shortcut
screen.](./media/image91.png)

3.  Angeben der Verbindungsdetails für die Verknüpfung:

[TABLE]

4.  Und klicken Sie auf **Next**.

![A screenshot of a computer Description automatically
generated](./media/image92.png)

5.  Dadurch wird eine Verknüpfung mit Ihrem Azure Storage-Container
    hergestellt. Wählen Sie den Speicher aus und klicken Sie auf die
    Schaltfläche **Next**.

![A screenshot of a computer Description automatically
generated](./media/image93.png)

![A screenshot of a computer Description automatically
generated](./media/image94.png)![A screenshot of a computer Description
automatically generated](./media/image95.png)

6.  Nachdem der Assistent gestartet wurde, wählen Sie **Files** und
    wählen Sie die Schaltfläche **"... "** auf der **Bronze** Datei.

![A screenshot of a computer Description automatically
generated](./media/image96.png)

7.  Wählen Sie **load to tables** und dann **new table** aus.

![](./media/image97.png)

8.  Geben Sie im Popup-Fenster den Namen für Ihre Tabelle **bronze_01**
    ein und wählen Sie den Dateityp als **Parquet** aus.

![A screenshot of a computer Description automatically
generated](./media/image98.png)

![A screenshot of a computer Description automatically
generated](./media/image99.png)

![A screenshot of a computer Description automatically
generated](./media/image100.png)

![A screenshot of a computer Description automatically
generated](./media/image101.png)

9.  Die Datei **bronze_01** ist nun in den Dateien.

![A screenshot of a computer Description automatically
generated](./media/image102.png)

10. Wählen Sie anschließend die **Option "... "** in der Bronze Datei.
    Wählen Sie **load to tables** und dann **existing table** aus.

![A screenshot of a computer Description automatically
generated](./media/image103.png)

11. Geben Sie den vorhandenen Tabellennamen als **dimcustomer_gold.**
    Wählen Sie den Dateityp als **Parquet** aus, und wählen Sie
    **load.**

![A screenshot of a computer Description automatically
generated](./media/image104.png)

![A screenshot of a computer Description automatically
generated](./media/image105.png)

## **Aufgabe 3: Erstellen eines semantischen Modells Verwenden der Goldschicht zum Erstellen eines Berichts**

In Ihrem Workspace können Sie jetzt die Goldebene verwenden, um einen
Bericht zu erstellen und die Daten zu analysieren. Sie können direkt in
Ihrem Workspace auf das semantische Modell zugreifen, um Beziehungen und
Measures für die Berichterstellung zu erstellen.

*Beachten Sie, dass Sie das **standardmäßige semantische Modell**, das
automatisch erstellt wird, wenn Sie ein Lakehouse erstellen, nicht
verwenden können. Sie müssen ein neues semantisches Modell erstellen,
das die Goldtabellen enthält, die Sie in diesem Lab aus dem
Lakehouse-Explorer erstellt haben.*

1.  Navigieren Sie in Ihrem Workspace zu Ihrem **WWIlakehouse**
    Lakehouse. Wählen Sie dann **New semantic Model** aus dem Menüband
    der Lakehouse-Explorer-Ansicht aus.

![A screenshot of a computer Description automatically
generated](./media/image106.png)

2.  Weisen Sie im Popup Ihrem neuen semantischen Modell den Namen
    **DatabricksTutorial** zu, und wählen Sie den Workspace als **Fabric
    Lakehouse Tutorial-29** aus.

![](./media/image107.png)

3.  Scrollen Sie anschließend nach unten, und wählen Sie Alle aus, die
    in Ihr semantisches Modell aufgenommen werden sollen, und wählen Sie
    **Confirm**.

Dadurch wird das semantische Modell in Fabric geöffnet, in dem Sie
Beziehungen und Measures erstellen können, wie hier gezeigt:

![A screenshot of a computer Description automatically
generated](./media/image108.png)

Von hier aus können Sie oder andere Mitglieder Ihres Datenteams Berichte
und Dashboards basierend auf den Daten in Ihrem Lakehouse erstellen.
Diese Berichte werden direkt mit der Goldschicht Ihres Lakehouse
verbunden, sodass sie immer die neuesten Daten widerspiegeln.

# Übung 5: Erfassen und Analysieren von Daten mit Azure Databricks

1.  Navigieren Sie im Power BI-Dienst zu Ihrem Lakehouse, und wählen Sie
    **\>Get Data** und dann **New Data Pipeline** aus.

![Screenshot showing how to navigate to new data pipeline option from
within the UI.](./media/image109.png)

1.  Geben Sie in der Eingabeaufforderung **New Pipeline** einen Namen
    für die neue Pipeline ein, und wählen Sie dann **Create**.
    **IngestDatapipeline01**

![](./media/image110.png)

2.  Wählen Sie für diese Übung die Beispieldaten **NYC Taxi - Green**
    als Datenquelle aus.

![A screenshot of a computer Description automatically
generated](./media/image111.png)

3.  Wählen Sie auf dem Vorschaubildschirm **Next**.

![A screenshot of a computer Description automatically
generated](./media/image112.png)

4.  Wählen Sie als Datenziel den Namen der Tabelle aus, die Sie zum
    Speichern der OneLake Delta-Tabellendaten verwenden möchten. Sie
    können eine vorhandene Tabelle auswählen oder eine neue erstellen.
    Wählen Sie für diese Übung **load into new table** und dann **Next**
    aus.

![A screenshot of a computer Description automatically
generated](./media/image113.png)

5.  Wählen Sie auf dem Bildschirm **Review + Save** die Option **Start
    data transfer immediately** und wählen Sie dann **Save + Run**.

![Screenshot showing how to enter table name.](./media/image114.png)

![A screenshot of a computer Description automatically
generated](./media/image115.png)

6.  Wenn der Auftrag abgeschlossen ist, navigieren Sie zu Ihrem
    Lakehouse, und zeigen Sie die Delta-Tabelle an, die unter /Tables
    aufgeführt ist.

![A screenshot of a computer Description automatically
generated](./media/image116.png)

7.  Kopieren Sie den ABFS-Pfad (Azure Blob Filesystem) in Ihre
    Deltatabelle, indem Sie in der Explorer-Ansicht mit der rechten
    Maustaste auf den Tabellennamen klicken und dann **Properties**.

![A screenshot of a computer Description automatically
generated](./media/image117.png)

8.  Öffnen Sie Ihr Azure Databricks Notebook, und führen Sie den Code
    aus.

olsPath = "**abfss://\<replace with workspace
name\>@onelake.dfs.fabric.microsoft.com/\<replace with item
name\>.Lakehouse/Tables/nycsample**"

df=spark.read.format('delta').option("inferSchema","true").load(olsPath)

df.show(5)

*Hinweis: Ersetzen Sie den fett formatierten Dateipfad durch den
kopierten Pfad.*

![](./media/image118.png)

9.  Aktualisieren Sie die Daten der Delta-Tabelle, indem Sie einen Field
    view ändern.

%sql

update delta.\`abfss://\<replace with workspace
name\>@onelake.dfs.fabric.microsoft.com/\<replace with item
name\>.Lakehouse/Tables/nycsample\` set vendorID = 99999 where vendorID
= 1;

*Hinweis: Ersetzen Sie den fett formatierten Dateipfad durch den
kopierten Pfad.*

![A screenshot of a computer Description automatically
generated](./media/image119.png)

# Übung 6: Bereinigen von Ressourcen

In dieser Übung haben Sie gelernt, wie Sie eine Medaillon-Architektur in
einem Microsoft Fabric-Lakehouse erstellen.

Wenn Sie mit der Erkundung Ihres Seehauses fertig sind, können Sie den
Arbeitsbereich löschen, den Sie für diese Übung erstellt haben.

1.  Wählen Sie Ihren Workspace, das **Fabric Lakehouse Tutorial-29,**
    aus dem linken Navigationsmenü aus. Es öffnet sich die Ansicht der
    Workspace Elemente.

![A screenshot of a computer Description automatically
generated](./media/image120.png)

2.  Wählen Sie die Option ***...*** unter dem Namen des Workspace und
    wählen Sie **Workspace settings**.

![A screenshot of a computer Description automatically
generated](./media/image121.png)

3.  Scrollen Sie nach unten und klicken Sie **Remove this workspace.**

![A screenshot of a computer Description automatically
generated](./media/image122.png)

4.  Klicken Sie in der Warnung, die sich öffnet**,** auf **Delete**.

![A white background with black text Description automatically
generated](./media/image123.png)

5.  Warten Sie auf eine Benachrichtigung, dass der Workspace gelöscht
    wurde, bevor Sie mit dem nächsten Lab fortfahren.

![A screenshot of a computer Description automatically
generated](./media/image124.png)

**Zusammenfassung**:

Dieses Lab führt die Teilnehmer durch das Erstellen einer
Medaillon-Architektur in einem Microsoft Fabric Lakehouse mithilfe von
Notebooks. Zu den wichtigsten Schritten gehören das Einrichten eines
Workspaces, das Einrichten eines Lakehouse, das Hochladen von Daten auf
die Bronze-Schicht für die erste Aufnahme, die Umwandlung in eine
silberne Delta-Tabelle für die strukturierte Verarbeitung, die weitere
Verfeinerung in goldene Delta-Tabellen für erweiterte Analysen, das
Untersuchen semantischer Modelle und das Erstellen von Datenbeziehungen
für eine aufschlussreiche Analyse.

## 
