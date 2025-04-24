# Caso d'uso 04: Analisi moderna su scala cloud con Azure Databricks e Microsoft Fabric

**Introduzione**

In questo lab si esplorerà l'integrazione di Azure Databricks con
Microsoft Fabric per creare e gestire un lakehouse usando l'architettura
Medallion, creare una tabella Delta con l'aiuto dell'account Azure Data
Lake Storage (ADLS) Gen2 usando Azure Databricks e inserire dati con
Azure Databricks. Questa guida pratica ti guiderà attraverso i passaggi
necessari per creare una lakehouse, caricare i dati al suo interno ed
esplorare i livelli di dati strutturati per facilitare l'analisi e la
reportistica efficienti dei dati.

L'architettura Medallion è composta da tre strati distinti (o zone).

- Bronzo: noto anche come zona grezza, questo primo livello memorizza i
  dati di origine nel loro formato originale. I dati in questo livello
  sono in genere di sola accodamento e non modificabili.

- Argento: noto anche come zona arricchita, questo livello memorizza i
  dati provenienti dallo strato di bronzo. I dati grezzi sono stati
  puliti e standardizzati e ora sono strutturati come tabelle (righe e
  colonne). Potrebbe anche essere integrato con altri dati per fornire
  una visione aziendale di tutte le entità aziendali, come cliente,
  prodotto e altri.

- Oro: noto anche come zona curata, questo livello finale memorizza i
  dati provenienti dal livello argento. I dati vengono perfezionati per
  soddisfare specifici requisiti aziendali e analitici a valle. Le
  tabelle sono in genere conformi alla progettazione dello schema a
  stella, che supporta lo sviluppo di modelli di dati ottimizzati per le
  prestazioni e l'usabilità.

**Obiettivi**:

- Comprendere i principi dell'architettura Medallion all'interno di
  Microsoft Fabric Lakehouse.

- Implementare un processo di gestione dei dati strutturati utilizzando
  i livelli Medallion (Bronzo, Argento, Oro).

- Trasformare i dati grezzi in dati convalidati e arricchiti per analisi
  e reportistica avanzate.

- Scoprire le best practice per la sicurezza dei dati, CI/CD e
  l'esecuzione efficiente delle query sui dati.

- Caricare i dati su OneLake con l'esploratore di file OneLake.

- Usare un notebook Fabric per leggere i dati in OneLake e riscriverli
  come tabella Delta.

- Analizzare e trasformare i dati con Spark usando un notebook Fabric.

- Interrogare una copia dei dati su OneLake con SQL.

- Creare una tabella Delta nell'account Azure Data Lake Storage (ADLS)
  Gen2 usando Azure Databricks.

- Creare un collegamento OneLake a una tabella Delta in ADLS.

- Usare Power BI per analizzare i dati tramite il collegamento ADLS.

- Leggere e modificare una tabella Delta in OneLake con Azure
  Databricks.

# Esercizio 1: Trasferimento dei dati di esempio in Lakehouse

In questo esercizio, verrà illustrato il processo di creazione di un
lakehouse e il caricamento dei dati al suo interno utilizzando Microsoft
Fabric.

Compito:sentiero

## **Attività 1: Creare un'area di lavoro Fabric**

In questa attività viene creata un'area di lavoro Fabric. L'area di
lavoro contiene tutti gli elementi necessari per questa esercitazione
lakehouse, che include lakehouse, flussi di dati, pipeline di Data
Factory, notebook, set di dati di Power BI e report.

1.  Aprire il browser, andare alla barra degli indirizzi e digitar o
    incollare il seguente URL:
    [https://app.fabric.microsoft.com/](https://app.fabric.microsoft.com/,)
    quindi premi il pulsante **Enter**.

> ![Una finestra del motore di ricerca con una casella rossa Descrizione
> generata automaticamente con confidenza media](./media/image1.png)

2.  Tornare alla finestra di **Power BI**. Nel menu di spostamento a
    sinistra della home page di Power BI spostarsi e fare clic su
    **Workspaces**.

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image2.png)

3.  Nel riquadro Aree di lavoro, fare clic sul pulsante **+** **New
    workspace.**

> ![A screenshot of a computer AI-generated content may be
> incorrect.](./media/image3.png)

4.  Nel riquadro **Create a workspace,** visualizzato sul lato destro,
    immettere i dettagli seguenti e fare clic sul pulsante **Apply**.

[TABLE]

> ![A screenshot of a computer AI-generated content may be
> incorrect.](./media/image4.png)

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image5.png)

5.  Attendere il completamento della distribuzione. Ci vogliono 2-3
    minuti per completarlo.

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image6.png)

## **Compito 2: Creare una lakehouse**

1.  Nella pagina **Power BI Fabric Lakehouse Tutorial-XX** fare clic
    sull'icona **Power BI** situata in basso a sinistra e selezionare
    **Data Engineering**.

> ![A screenshot of a computer AI-generated content may be
> incorrect.](./media/image7.png)

2.  Nella home page di **Synapse Data Engineering,** selezionare
    **Lakehouse** per creare una lakehouse.

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image8.png)

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image9.png)

3.  Nella finestra di dialogo **New lakehouse** , inserire
    **wwilakehouse** nel campo **Name**, fare clic sul pulsante
    **Create** e aprire la nuova lakehouse.

> **NOTA**: Assicurati di rimuovere lo spazio prima di **wwilakehouse**.
>
> ![A screenshot of a computer AI-generated content may be
> incorrect.](./media/image10.png)
>
> ![Uno screenshot di un computer Descrizione generata
> automaticamente](./media/image11.png)
>
> ![A screenshot of a computer AI-generated content may be
> incorrect.](./media/image12.png)

4.  Verrà visualizzata una notifica che indica **Successfully created
    SQL endpoint**.

> ![A screenshot of a computer AI-generated content may be
> incorrect.](./media/image13.png)

# Esercizio 2: Implementazione dell'architettura Medallion con Azure Databricks

## **Attività 1: Impostazione del livello di bronzo**

1.  Nella pagina **wwilakehouse**, selezionare l'icona More accanto ai
    file (...) e selezionar **New subfolder**

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image14.png)

2.  Nella finestra a comparsa, specificare il nome della cartella come
    **bronze** e selezionare **Create**.

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image15.png)

3.  A questo punto, selezionare l'icona More accanto ai file di bronzo
    (...) e selezionare **Upload**, quindi **upload files**.

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image16.png)

4.  Nel riquadro di **upload file,** selezionare il pulsante di opzione
    **Upload file**. Fare clic sul pulsante **Browse** e accedere a
    **C:\LabFiles**, quindi selezionare il file dei file di dati di
    vendita richiesto (2019, 2020, 2021) e fare clic sul pulsante
    **Open**.

Quindi, selezionare **Upload** per caricare i file nella nuova cartella
"bronze" nella tua Lakehouse.

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image17.png)

> ![Uno screenshot di un computer Descrizione generata
> automaticamente](./media/image18.png)

5.  Fare clic sulla cartella **bronze** per verificare che i file siano
    stati caricati correttamente e che i file riflettano.

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image19.png)

# Esercizio 3: Trasformazione dei dati con Apache Spark ed esecuzione di query con SQL nell'architettura medallion 

## **Attività 1: Trasformare i dati e caricarli in una tabella Delta d'argento**

Nella pagina **wwilakehouse**, navigare e fare clic su **Open notebook**
nella barra dei comandi, quindi seleziona **New notebook**.

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image20.png)

1.  Selezionare la prima cella (che attualmente è una cella di
    *codice*), quindi nella barra degli strumenti dinamica in alto a
    destra, usa il pulsante **M↓** per **convertire la cella in una
    cella markdown**.

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image21.png)

2.  Quando la cella si trasforma in una cella markdown, viene eseguito
    il rendering del testo in essa contenuto.

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image22.png)

3.  Utilizzare il **🖉** pulsante (Modifica) per passare la cella alla
    modalità di modifica, sostituire tutto il testo, quindi modificare
    il markdown come segue:

CodeCopy 

\# Sales order data exploration

Utilizzare il codice in questo blocco appunti per esplorare i dati degli
ordini di vendita.

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image23.png)

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image24.png)

4.  Fare clic in un punto qualsiasi del notebook all'esterno della cella
    per interrompere la modifica e visualizzare il markdown di cui è
    stato eseguito il rendering.

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image25.png)

5.  Utilizzare l'icona **+ Code** sotto l'output della cella per
    aggiungere una nuova cella di codice al notebook.

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image26.png)

6.  A questo punto, usare il notebook per caricare i dati dal livello di
    bronzo in un DataFrame Spark.

Selezionare la cella esistente nel notebook, che contiene un semplice
codice commentato. Evidenzia ed elimina queste due righe: non avrai
bisogno di questo codice.

*Nota: i notebook consentono di eseguire codice in una varietà di
linguaggi, tra cui Python, Scala e SQL. In questo esercizio si useranno
PySpark e SQL. Puoi anche aggiungere celle markdown per fornire testo e
immagini formattati per documentare il tuo codice.*

Per questo, inserire il seguente codice e fare clic su **Run**.

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

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image27.png)

***Nota**: poiché questa è la prima volta che esegui un codice Spark in
questo notebook, è necessario avviare una sessione Spark. Ciò significa
che il completamento della prima esecuzione può richiedere circa un
minuto. Le corse successive saranno più veloci.*

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image28.png)

7.  Il codice eseguito ha caricato i dati dai file CSV nella cartella
    **bronze** in un frame di dati Spark e quindi ha visualizzato le
    prime righe del frame di dati.

> **Nota**: è possibile cancellare, nascondere e ridimensionare
> automaticamente il contenuto dell'output della cella selezionando
> l'icona **...** in alto a sinistra nel riquadro di output.

8.  A questo punto si **aggiungeranno** **colonne per la convalida e la
    pulizia dei dati**, usando un frame di dati PySpark per aggiungere
    colonne e aggiornare i valori di alcune delle colonne esistenti.
    Utilizzare il pulsante + per **aggiungere un nuovo blocco di
    codice** e aggiungere il seguente codice alla cella:

> CodeCopy
>
> from pyspark.sql.functions import when, lit, col, current_timestamp,
> input_file_name 
>
>      
>
>  # Add columns IsFlagged, CreatedTS and ModifiedTS 
>
>  df = df.withColumn("FileName", input_file_name()) \\
>
>      .withColumn("IsFlagged", when(col("OrderDate") \<
> '2019-08-01',True).otherwise(False)) \\
>
>      .withColumn("CreatedTS",
> current_timestamp()).withColumn("ModifiedTS", current_timestamp()) 
>
>      
>
>  # Update CustomerName to "Unknown" if CustomerName null or empty 
>
>  df = df.withColumn("CustomerName", when((col("CustomerName").isNull()
> |
> (col("CustomerName")=="")),lit("Unknown")).otherwise(col("CustomerName")))
>
> La prima riga del codice importa le funzioni necessarie da PySpark. Si
> aggiungono quindi nuove colonne al frame di dati in modo da poter
> tenere traccia del nome del file di origine, se l'ordine è stato
> contrassegnato come precedente all'anno fiscale di interesse e quando
> la riga è stata creata e modificata.
>
> Infine, si aggiorna la colonna CustomerName a "Unknown" se è null o
> vuota.
>
> Quindi, esegui la cella per eseguire il codice utilizzando il pulsante
> **\*\*▷** (*Run cell*)\*\*.

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image29.png)

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image30.png)

9.  Successivamente, si definirà lo schema per la tabella
    **sales_silver** nel database delle vendite utilizzando il formato
    Delta Lake. Creare un nuovo blocco di codice e aggiungere il
    seguente codice alla cella:

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

   

10. Eseguire la cella per eseguire il codice utilizzando il pulsante
    **\*\*▷** (*Run cell*)\*\*.

11. Seleziona l'icona **...** nella sezione Tables del riquadro
    Lakehouse Explorer e selezionare **Refresh**. A questo punto
    dovrebbe essere elencata la nuova tabella **sales_silver**. Il **▲**
    (icona a forma di triangolo) indica che si tratta di una tabella
    Delta.

> **Nota**: se la nuova tabella non viene visualizzata, attendere
> qualche secondo, quindi selezionare nuovamente **Refresh ** oppure
> aggiornare l'intera scheda del browser.
>
> ![Uno screenshot di un computer Descrizione generata
> automaticamente](./media/image31.png)
>
> ![Uno screenshot di un computer Descrizione generata
> automaticamente](./media/image32.png)

12. A questo punto si eseguirà un'**operazione di upsert** su una
    tabella Delta, aggiornando i record esistenti in base a condizioni
    specifiche e inserendo nuovi record quando non viene trovata alcuna
    corrispondenza. Aggiungere un nuovo blocco di codice e incollare il
    codice seguente:

> CodeCopy
>
> from pyspark.sql.types import \* 
>
> from pyspark.sql.functions import when, lit, col, current_timestamp,
> input_file_name 
>
> from delta.tables import \* 
>
>  
>
> \# Define the schema for the source data 
>
> orderSchema = StructType(\[ 
>
>     StructField("SalesOrderNumber", StringType(), True), 
>
>     StructField("SalesOrderLineNumber", IntegerType(), True), 
>
>     StructField("OrderDate", DateType(), True), 
>
>     StructField("CustomerName", StringType(), True), 
>
>     StructField("Email", StringType(), True), 
>
>     StructField("Item", StringType(), True), 
>
>     StructField("Quantity", IntegerType(), True), 
>
>     StructField("UnitPrice", FloatType(), True), 
>
>     StructField("Tax", FloatType(), True) 
>
> \]) 
>
>  
>
> \# Read data from the bronze folder into a DataFrame 
>
> df = spark.read.format("csv").option("header",
> "true").schema(orderSchema).load("Files/bronze/\*.csv") 
>
>  
>
> \# Add additional columns 
>
> df = df.withColumn("FileName", input_file_name()) \\
>
>     .withColumn("IsFlagged", when(col("OrderDate") \< '2019-08-01',
> True).otherwise(False)) \\
>
>     .withColumn("CreatedTS", current_timestamp()) \\
>
>     .withColumn("ModifiedTS", current_timestamp()) \\
>
>     .withColumn("CustomerName", when((col("CustomerName").isNull()) |
> (col("CustomerName") == ""),
> lit("Unknown")).otherwise(col("CustomerName"))) 
>
>  
>
> \# Define the path to the Delta table 
>
> deltaTablePath = "Tables/sales_silver" 
>
>  
>
> \# Create a DeltaTable object for the existing Delta table 
>
> deltaTable = DeltaTable.forPath(spark, deltaTablePath) 
>
>  
>
> \# Perform the merge (upsert) operation 
>
> deltaTable.alias('silver') \\
>
>     .merge( 
>
>         df.alias('updates'), 
>
>         'silver.SalesOrderNumber = updates.SalesOrderNumber AND \\
>
>          silver.OrderDate = updates.OrderDate AND \\
>
>          silver.CustomerName = updates.CustomerName AND \\
>
>          silver.Item = updates.Item' 
>
>     ) \\
>
>     .whenMatchedUpdate(set = { 
>
>         "SalesOrderLineNumber": "updates.SalesOrderLineNumber", 
>
>         "Email": "updates.Email", 
>
>         "Quantity": "updates.Quantity", 
>
>         "UnitPrice": "updates.UnitPrice", 
>
>         "Tax": "updates.Tax", 
>
>         "FileName": "updates.FileName", 
>
>         "IsFlagged": "updates.IsFlagged", 
>
>         "ModifiedTS": "current_timestamp()" 
>
>     }) \\
>
>     .whenNotMatchedInsert(values = { 
>
>         "SalesOrderNumber": "updates.SalesOrderNumber", 
>
>         "SalesOrderLineNumber": "updates.SalesOrderLineNumber", 
>
>         "OrderDate": "updates.OrderDate", 
>
>         "CustomerName": "updates.CustomerName", 
>
>         "Email": "updates.Email", 
>
>         "Item": "updates.Item", 
>
>         "Quantity": "updates.Quantity", 
>
>         "UnitPrice": "updates.UnitPrice", 
>
>         "Tax": "updates.Tax", 
>
>         "FileName": "updates.FileName", 
>
>         "IsFlagged": "updates.IsFlagged", 
>
>         "CreatedTS": "current_timestamp()", 
>
>         "ModifiedTS": "current_timestamp()" 
>
>     }) \\
>
>     .execute() 

13. Eseguire la cella per eseguire il codice utilizzando il pulsante
    **\*\*▷** (*Eseguire cella*)\*\*.

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image33.png)

Questa operazione è importante perché consente di aggiornare i record
esistenti nella tabella in base ai valori di colonne specifiche e di
inserire nuovi record quando non viene trovata alcuna corrispondenza. Si
tratta di un requisito comune quando si caricano dati da un sistema di
origine che può contenere aggiornamenti a record esistenti e nuovi.

A questo punto sono presenti dati nella tabella delta d'argento pronti
per ulteriori trasformazioni e modellazioni.

I dati sono stati prelevati dal livello di bronzo, li sono stati
trasformati e caricati in una tabella Delta in argento. A questo punto
si userà un nuovo notebook per trasformare ulteriormente i dati,
modellarli in uno schema a stella e caricarli in tabelle Delta dorate.

*Si noti che tutte queste operazioni potrebbero essere state eseguite in
un unico blocco appunti, ma ai fini di questo esercizio si utilizzano
blocchi appunti separati per illustrare il processo di trasformazione
dei dati da bronzo ad argento e quindi da argento a oro. Questo può
aiutare con il debug, la risoluzione dei problemi e il riutilizzo*.

## **Attività 2: Caricare i dati nelle tabelle Gold Delta**

1.  Tornare alla home page di Fabric Lakehouse Tutorial-29.

> ![Uno screenshot di un computer Descrizione generata
> automaticamente](./media/image34.png)

2.  Selezionare **wwilakehouse.**

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image35.png)

3.  Nel riquadro di esplorazione lakehouse, dovrebbe essere visualizzata
    la tabella **sales_silver** elencata nella sezione **Tables **del
    riquadro di esplorazione.

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image36.png)

4.  A questo punto, creare un nuovo notebook denominato **Transform data
    for Gold**. Per questo, navigare e fare clic su **Open notebook**
    nella barra dei comandi, quindi selezionare **New notebook**.

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image37.png)

5.  Nel blocco di codice esistente, rimuovi il testo standard e
    **aggiungi il seguente codice** per caricare i dati nel tuo
    dataframe e iniziare a creare il tuo schema a stella, quindi
    eseguilo:

> CodeCopy

\# Load data to the dataframe as a starting point to create the gold
layer 

df = spark.read.table("wwilakehouse.sales_silver") 

 

\# Display the first few rows of the dataframe to verify the data 

df.show() 

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image38.png)

6.  Successivamente**, Add a new code block** e incollare il seguente
    codice per creare la tabella della dimensione della data ed
    eseguirla:

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

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image39.png)

**Nota**: È possibile eseguire il commando display(df) in qualsiasi
momento per controllare l'avanzamento del lavoro. In questo caso,
eseguiresti 'display(dfdimDate_gold)' per vedere il contenuto del
dataframe dimDate_gold.

7.  In un nuovo blocco di codice, **add and run the following code** per
    creare un dataframe per la dimensione data, **dimdate_gold**:

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

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image40.png)

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image41.png)

8.  Si separa il codice in nuovi blocchi di codice in modo da poter
    comprendere e osservare ciò che accade nel notebook durante la
    trasformazione dei dati. In un altro nuovo blocco di codice, ** add
    and run the following code** per aggiornare la dimensione della data
    man mano che arrivano nuovi dati:

> CodeCopy
>
>  from delta.tables import \* 
>
>      
>
>  deltaTable = DeltaTable.forPath(spark, 'Tables/dimdate_gold') 
>
>      
>
>  dfUpdates = dfdimDate_gold 
>
>      
>
>  deltaTable.alias('silver') \\
>
>    .merge( 
>
>      dfUpdates.alias('updates'), 
>
>      'silver.OrderDate = updates.OrderDate' 
>
>    ) \\
>
>     .whenMatchedUpdate(set = 
>
>      { 
>
>            
>
>      } 
>
>    ) \\
>
>   .whenNotMatchedInsert(values = 
>
>      { 
>
>        "OrderDate": "updates.OrderDate", 
>
>        "Day": "updates.Day", 
>
>        "Month": "updates.Month", 
>
>        "Year": "updates.Year", 
>
>        "mmmyyyy": "updates.mmmyyyy", 
>
>        "yyyymm": "yyyymm" 
>
>      } 
>
>    ) \\
>
>    .execute() 

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image42.png)

> La dimensione della data è configurata.

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image43.png)

## **Attività 3: Creare la dimensione cliente.**

1.  Per creare la tabella delle dimensioni del cliente, **add a new code
    block**, incollare ed eseguire il seguente codice:

> CodeCopy

 from pyspark.sql.types import \* 

 from delta.tables import \* 

     

 # Create customer_gold dimension delta table 

 DeltaTable.createIfNotExists(spark) \\

     .tableName("wwilakehouse.dimcustomer_gold") \\

     .addColumn("CustomerName", StringType()) \\

     .addColumn("Email",  StringType()) \\

     .addColumn("First", StringType()) \\

     .addColumn("Last", StringType()) \\

     .addColumn("CustomerID", LongType()) \\

     .execute() 

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image44.png)

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image45.png)

2.  In un nuovo blocco di codice, **add and run the following code** per
    eliminare i clienti duplicati, selezionare colonne specifiche e
    dividere la colonna "CustomerName" per creare le colonne "Nome" e
    "Cognome":

> CodeCopy
>
>  from pyspark.sql.functions import col, split 
>
>      
>
>  # Create customer_silver dataframe 
>
>      
>
>  dfdimCustomer_silver =
> df.dropDuplicates(\["CustomerName","Email"\]).select(col("CustomerName"),col("Email"))
> \\
>
>      .withColumn("First",split(col("CustomerName"), " ").getItem(0))
> \\
>
>      .withColumn("Last",split(col("CustomerName"), " ").getItem(1))  
>
>      
>
>  # Display the first 10 rows of the dataframe to preview your data 
>
>  
>
>  display(dfdimCustomer_silver.head(10)) 

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image46.png)

Qui è stato creato un nuovo dfdimCustomer_silver DataFrame eseguendo
varie trasformazioni, ad esempio l'eliminazione di duplicati, la
selezione di colonne specifiche e la suddivisione della colonna
"CustomerName" per creare colonne "Nome" e "Cognome". Il risultato è un
DataFrame con dati dei clienti puliti e strutturati, incluse le colonne
separate "Nome" e "Cognome" estratte dalla colonna "CustomerName".

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image47.png)

3.  Successivamente, creeremo **la colonna ID per i nostri clienti**. In
    un nuovo blocco di codice, incollare ed eseguire quanto segue:

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

 

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image48.png)

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image49.png)

4.  Ora ti assicurerai che la tua tabella dei clienti rimanga aggiornata
    man mano che arrivano nuovi dati. **In a new code block**, incollare
    ed eseguire quanto segue:

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

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image51.png)

5.  Ora ripeterai **questi passaggi per creare la dimensione del
    prodotto**. In un nuovo blocco di codice, incollare ed eseguire
    quanto segue:

> CodeCopy
>
> from pyspark.sql.types import \* 
>
> from delta.tables import \* 
>
>      
>
> DeltaTable.createIfNotExists(spark) \\
>
>     .tableName("wwilakehouse.dimproduct_gold") \\
>
>     .addColumn("ItemName", StringType()) \\
>
>     .addColumn("ItemID", LongType()) \\
>
>     .addColumn("ItemInfo", StringType()) \\
>
>     .execute() 

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image52.png)

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image53.png)

6.  **Aggiungere un altro blocco di codice** per creare il
    **product_silver** dataframe.

> CodeCopy
>
> from pyspark.sql.functions import col, split, lit 
>
>      
>
> \# Create product_silver dataframe 
>
>      
>
> dfdimProduct_silver =
> df.dropDuplicates(\["Item"\]).select(col("Item")) \\
>
>     .withColumn("ItemName",split(col("Item"), ", ").getItem(0)) \\
>
>     .withColumn("ItemInfo",when((split(col("Item"), ",
> ").getItem(1).isNull() | (split(col("Item"), ",
> ").getItem(1)=="")),lit("")).otherwise(split(col("Item"), ",
> ").getItem(1)))  
>
>      
>
> \# Display the first 10 rows of the dataframe to preview your data 
>
>  
>
> display(dfdimProduct_silver.head(10)) 

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image54.png)

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image55.png)

7.  A questo punto creerai gli ID per la tua **dimProduct_gold table**.
    Aggiungere la sintassi seguente a un nuovo blocco di codice ed
    eseguirlo:

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

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image56.png)

In questo modo viene calcolato il successivo ID prodotto disponibile in
base ai dati correnti nella tabella, vengono assegnati questi nuovi ID
ai prodotti e quindi vengono visualizzate le informazioni aggiornate sul
prodotto.

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image57.png)

8.  Analogamente a quanto hai fatto con le altre dimensioni, devi
    assicurarti che la tabella dei prodotti rimanga aggiornata man mano
    che arrivano nuovi dati. **In un nuovo blocco di codice**, incollare
    ed eseguire quanto segue:

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

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image58.png)

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image59.png)

**Dopo aver creato le dimensioni, il passaggio finale consiste nel
creare la tabella dei fatti.**

9.  **In un nuovo blocco di codice** incollare ed eseguire il codice
    seguente per creare la  **fact table**:

> CodeCopy
>
> from pyspark.sql.types import \* 
>
> from delta.tables import \* 
>
>      
>
> DeltaTable.createIfNotExists(spark) \\
>
>     .tableName("wwilakehouse.factsales_gold") \\
>
>     .addColumn("CustomerID", LongType()) \\
>
>     .addColumn("ItemID", LongType()) \\
>
>     .addColumn("OrderDate", DateType()) \\
>
>     .addColumn("Quantity", IntegerType()) \\
>
>     .addColumn("UnitPrice", FloatType()) \\
>
>     .addColumn("Tax", FloatType()) \\
>
>     .execute() 

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image60.png)

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image61.png)

10. **In un nuovo blocco di codice**, incollare ed eseguire il seguente
    codice per creare un **new dataframe** per combinare i dati di
    vendita con le informazioni sul cliente e sul prodotto, tra cui l'ID
    cliente, l'ID articolo, la data dell'ordine, la quantità, il prezzo
    unitario e l'imposta:

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

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image62.png)

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image63.png)

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image64.png)

1.  A questo punto è possibile assicurarsi che i dati di vendita
    rimangano aggiornati eseguendo il codice seguente in un** new code
    block**:

> CodeCopy
>
> from delta.tables import \* 
>
>      
>
> deltaTable = DeltaTable.forPath(spark, 'Tables/factsales_gold') 
>
>      
>
> dfUpdates = dffactSales_gold 
>
>      
>
> deltaTable.alias('silver') \\
>
>   .merge( 
>
>     dfUpdates.alias('updates'), 
>
>     'silver.OrderDate = updates.OrderDate AND silver.CustomerID =
> updates.CustomerID AND silver.ItemID = updates.ItemID' 
>
>   ) \\
>
>    .whenMatchedUpdate(set = 
>
>     { 
>
>            
>
>     } 
>
>   ) \\
>
>  .whenNotMatchedInsert(values = 
>
>     { 
>
>       "CustomerID": "updates.CustomerID", 
>
>       "ItemID": "updates.ItemID", 
>
>       "OrderDate": "updates.OrderDate", 
>
>       "Quantity": "updates.Quantity", 
>
>       "UnitPrice": "updates.UnitPrice", 
>
>       "Tax": "updates.Tax" 
>
>     } 
>
>   ) \\
>
>   .execute() 

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image65.png)

In questo caso si utilizza l'operazione di unione di Delta Lake per
sincronizzare e aggiornare la tabella factsales_gold con i nuovi dati di
vendita (dffactSales_gold). L'operazione confronta la data dell'ordine,
l'ID cliente e l'ID articolo tra i dati esistenti (tabella argento) e i
nuovi dati (aggiorna DataFrame), aggiornando i record corrispondenti e
inserendo nuovi record in base alle esigenze.

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image66.png)

A questo punto si dispone di un livello **d'oro** curato e modellato che
può essere utilizzato per la creazione di report e l'analisi.

# Esercizio 4: Stabilire la connettività tra Azure Databricks e Azure Data Lake Storage (ADLS) Gen 2

Creiamo ora una tabella Delta con l'aiuto dell'account Azure Data Lake
Storage (ADLS) Gen2 usando Azure Databricks. Si creerà quindi un
collegamento OneLake a una tabella Delta in ADLS e si userà Power BI per
analizzare i dati tramite il collegamento ADLS.

## **Attività 0: Riscattare un pass di Azure e abilitare la sottoscrizione di Azure**

1.  Navigare sul seguente link !! https://www.microsoftazurepass.com/!!
    e fare clic sul pulsante **Start**.

![](./media/image67.png)

2.  Nella pagina di accesso di Microsoft, inserire **Tenant ID,** fare
    clic su **Next**.

![](./media/image68.png)

3.  Nella pagina successiva inserire la tua password e fare clic su
    **Sign In**.

![A screenshot of a login box AI-generated content may be
incorrect.](./media/image69.png)

![Uno screenshot di un errore del computer Descrizione generata
automaticamente](./media/image70.png)

4.  Una volta effettuato l'accesso, nella pagina Microsoft Azure, fare
    clic sulla scheda **Confirm Microsoft Account**.

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image71.png)

5.  Nella pagina successiva, inserire il codice promozionale, i
    caratteri Captcha e cliccare su **Submit.**

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image72.png)

![Uno screenshot di un errore del computer Descrizione generata
automaticamente](./media/image73.png)

6.  Nella pagina Il tuo profilo inserire i dettagli del tuo profilo e
    cliccare su **Sign up.**

7.  se richiesto, iscriversi all'autenticazione a più fattori e quindi
    accedere al portale Azure navigando fino al seguente link!!
    <https://portal.azure.com/#home>!!

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image74.png)

8.  Nella barra di ricerca, digitare **Subscription** e fare clic
    sull'icona **Subscription** sotto **Services.**

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image75.png)

9.  Dopo il riscatto di Azure Pass, verrà generato un ID sottoscrizione.

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image76.png)

## **Attività 1: Creare un account Azure Data Storage**

1.  Accedere al portale di Azure usando le credenziali di Azure.

2.  Nella home page, dal menu del portale a sinistra, selezionare
    **Storage accounts** per visualizzare un elenco degli account di
    archiviazione. Se il menu del portale non è visibile, selezionare il
    pulsante del menu per attivarlo.

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image77.png)

3.  Nella pagina **Storage accounts,** selezionare **Create**.

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image78.png)

4.  Nella scheda Basics, dopo aver selezionato un gruppo di risorse,
    specificare le informazioni essenziali per l'account di
    archiviazione:

[TABLE]

Lasciare invariate le altre impostazioni e selezionare **Review +
create** per accettare le opzioni predefinite e procedere con la
convalida e la creazione dell'account.

Nota: Se non è già stato creato un gruppo di risorse, è possibile fare
clic su " **Create new**" e creare una nuova risorsa per l'account di
archiviazione.

![](./media/image79.png)

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image80.png)

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image81.png)

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image82.png)

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image83.png)

5.  Quando si passa alla scheda **Review + create**, Azure esegue la
    convalida sulle impostazioni dell'account di archiviazione scelte.
    Se la convalida viene superata, è possibile procedere alla creazione
    dell'account di archiviazione.

Se la convalida non riesce, il portale indica quali impostazioni devono
essere modificate.

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image84.png)

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image85.png)

A questo punto è stato creato correttamente l'account di archiviazione
dati di Azure.

6.  Passare alla pagina degli account di archiviazione eseguendo una
    ricerca sulla barra di ricerca nella parte superiore della pagina,
    selezionare l'account di archiviazione appena creato.

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image86.png)

7.  Nella pagina dell'account di archiviazione passare a **Containers**
    in **Data storage** nel riquadro di spostamento a sinistra per
    creare un nuovo contenitore con il nome !!medalion1!! e fare clic
    sul pulsante **Create**.

 

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image87.png)

8.  A questo punto tornare alla pagina **storage account**, selezionare
    **Endpoints** dal menu di spostamento a sinistra. Scorrere verso il
    basso e copiare **Primary endpoint URL** e incollarlo in un blocco
    note. Questo sarà utile durante la creazione del collegamento.

![](./media/image88.png)

9.  Allo stesso modo, andare ai **Access keys** sullo stesso pannello di
    navigazione.

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image89.png)

## 

## **Attività 2: Creare una tabella Delta, creare un collegamento e analizzare i dati nel Lakehouse**

1.  Nella tua lakehouse, selezionare le ellissi **(...)** accanto a
    file, quindi selezionare **New shortcut**.

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image90.png)

2.  Nella schermata **New shortcut,** selezionare il riquadro **Azure
    Data Lake Storage Gen2**.

![Screenshot delle opzioni del riquadro nella schermata Nuova scelta
rapida.](./media/image91.png)

3.  Specificare i dettagli di connessione per il collegamento:

[TABLE]

4.  E fare clic su **Next**.

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image92.png)

5.  In questo modo verrà stabilito un collegamento con il contenitore di
    archiviazione di Azure. Selezionare lo spazio di archiviazione e
    selezionare il pulsante **Next**.

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image93.png)

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image94.png)![Uno screenshot di un computer
Descrizione generata automaticamente](./media/image95.png)

6.  Una volta avviata la procedura guidata, selezionare **Files** e
    selezionare l'icona **"... "** sulla lima di **bronze**.

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image96.png)

7.  Selezionare **load to tables** e **New table**.

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image97.png)

8.  Nella finestra pop-up, fornire il nome della tua tabella come
    **bronze_01** e selezionare il tipo di file come **parquet**.

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image98.png)

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image99.png)

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image100.png)

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image101.png)

9.  Il file **bronze_01** è ora visibile nei file.

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image102.png)

10. Quindi, selezionare l'icona **"... "** sul file di **bronze**.
    Selezionare **load to tables** e **existing table.**

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image103.png)

11. Specificare il nome della tabella esistente come
    **dimcustomer_gold.** Selezionare il tipo di file come **parquet** e
    selezionare **load.**

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image104.png)

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image105.png)

## **Attività 3: Creare un modello Semantico utilizzando livello oro per creare un report**

Nell'area di lavoro, è ora possibile utilizzare il livello oro per
creare un report e analizzare i dati. È possibile accedere al modello
semantico direttamente nell'area di lavoro per creare relazioni e misure
per la creazione di report.

*Si noti che non è possibile utilizzare il **modello semantico
predefinito** che viene creato automaticamente quando si crea una
lakehouse. È necessario creare un nuovo modello semantico che includa le
tabelle gold create in questo lab, dal lakehouse explorer.*

1.  Nel tuo spazio di lavoro, andare alla tua lakehouse
    **wwilakehouse**. Selezionare quindi **New semantic model** dalla
    barra multifunzione della visualizzazione explorer lakehouse.

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image106.png)

2.  Nel popup, assegnare il nome **DatabricksTutorial** al nuovo modello
    semantico e selezionare l'area di lavoro come **Fabric Lakehouse
    Tutorial-29**.

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image107.png)

3.  Quindi, scorrere verso il basso e selezionare tutto da includere nel
    tuo modello semantico e selezionare **Confirm**.

Verrà aperto il modello semantico in Fabric in cui è possibile creare
relazioni e misure, come illustrato di seguito:

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image108.png)

Da qui, tu o altri membri del tuo team dati potete creare report e
dashboard basati sui dati nel tuo lakehouse. Questi report saranno
collegati direttamente allo strato d'oro della tua casa sul lago, in
modo da riflettere sempre i dati più recenti.

# Esercizio 5: Inserimento di dati e analisi con Azure Databricks

1.  Passare al lakehouse nel servizio Power BI e selezionare **Get
    data** e quindi selezionare **New data pipeline**.

![Screenshot che mostra come passare alla nuova opzione della pipeline
di dati dall'interfaccia utente.](./media/image109.png)

1.  Nel prompt ** New Pipeline** immettere un nome per la nuova pipeline
    e quindi selezionare **Create**. **IngestDatapipeline01**

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image110.png)

2.  Per questo esercizio, selezionare i dati di esempio **NYC Taxi -
    Green** come origine dati.

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image111.png)

3.  Nella schermata di anteprima, selezionare **Next**.

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image112.png)

4.  Per Destinazione dati, selezionare il nome della tabella che si
    desidera utilizzare per archiviare i dati della tabella Delta di
    OneLake. È possibile scegliere una tabella esistente o crearne una
    nuova. Ai fini di questo lab, selezionare **load into new table** e
    selezionare **Next**.

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image113.png)

5.  Nella schermata ** Review + Save,** selezionare **Start data
    transfer immediately** e quindi selezionare ** Save + Run**.

![Schermata che mostra come inserire il nome della
tabella.](./media/image114.png)

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image115.png)

6.  Al termine del processo, accedere alla lakehouse e visualizzare la
    tabella delta elencata in /Tables.

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image116.png)

7.  Copiare il percorso ABFS (Azure Blob Filesystem) nella tabella delta
    facendo clic con il pulsante destro del mouse sul nome della tabella
    nella visualizzazione Esplora risorse e scegliendo **Properties**.

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image117.png)

8.  Aprire il notebook di Azure Databricks ed eseguire il codice.

olsPath = "**abfss://\<replace with workspace
name\>@onelake.dfs.fabric.microsoft.com/\<replace with item
name\>.Lakehouse/Tables/nycsample**"  

df=spark.read.format('delta').option("inferSchema","true").load(olsPath) 

df.show(5) 

*Nota: sostituire il percorso del file in grassetto con quello che hai
copiato.*

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image118.png)

9.  Aggiornare i dati della tabella Delta modificando il valore di un
    campo.

%sql 

update delta.\`abfss://\<replace with workspace
name\>@onelake.dfs.fabric.microsoft.com/\<replace with item
name\>.Lakehouse/Tables/nycsample\` set vendorID = 99999 where vendorID
= 1; 

*Nota: sostituire il percorso del file in grassetto con quello che hai
copiato.*

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image119.png)

# Esercizio 6: Pulire le risorse

In questo esercizio si è appreso come creare un'architettura medallion
in una lakehouse di Microsoft Fabric.

Se hai finito di esplorare la tua casa sul lago, puoi eliminare l'area
di lavoro che hai creato per questo esercizio.

1.  Seleziona il tuo spazio di lavoro, il **Fabric Lakehouse
    Tutorial-29** dal menu di navigazione a sinistra. Apre la
    visualizzazione degli elementi dell'area di lavoro.

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image120.png)

2.  Selezionare l'icona ***...*** sotto il nome dell'area di lavoro e
    selezionare **Workspace settings**.

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image121.png)

3.  Scorrere verso il basso e selezionare **Remove this workspace.**

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image122.png)

4.  Fare clic su **Delete** nell'avviso che si apre.

![Uno sfondo bianco con testo nero Descrizione generata
automaticamente](./media/image123.png)

5.  Attendere una notifica che indica che l'area di lavoro è stata
    eliminata prima di passare al lab successivo.

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image124.png)

**Sommario**:

Questo lab guida i partecipanti nella creazione di un'architettura
medallion in una lakehouse di Microsoft Fabric usando notebooks. I
passaggi chiave includono la configurazione di un'area di lavoro, la
creazione di una lakehouse, il caricamento dei dati sul livello di
bronzo per l'acquisizione iniziale, la trasformazione in una tabella
Delta argentata per l'elaborazione strutturata, l'ulteriore
perfezionamento in tabelle Delta dorate per analisi avanzate,
l'esplorazione di modelli semantici e la creazione di relazioni tra i
dati per un'analisi approfondita.

## 
