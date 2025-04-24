**Introduzione**

Apache Spark è un motore open source per l'elaborazione distribuita dei
dati ed è ampiamente utilizzato per esplorare, elaborare e analizzare
enormi volumi di dati nell'archiviazione del data lake. Spark è
disponibile come opzione di elaborazione in molti prodotti della
piattaforma dati, tra cui Azure HDInsight, Azure Databricks, Azure
Synapse Analytics e Microsoft Fabric. Uno dei vantaggi di Spark è il
supporto per un'ampia gamma di linguaggi di programmazione, tra cui
Java, Scala, Python e SQL; rendendo Spark una soluzione molto flessibile
per i carichi di lavoro di elaborazione dei dati, tra cui la pulizia e
la manipolazione dei dati, l'analisi statistica e l'apprendimento
automatico, l'analisi e la visualizzazione dei dati.

Le tabelle in un lakehouse di Microsoft Fabric si basano sul formato
Delta Lake open source per Apache Spark. Delta Lake aggiunge il supporto
per la semantica relazionale per le operazioni di dati batch e di
streaming e consente la creazione di un'architettura Lakehouse in cui
Apache Spark può essere usato per elaborare ed eseguire query sui dati
in tabelle basate su file sottostanti in un data lake.

In Microsoft Fabric, i flussi di dati (Gen2) si connettono a varie
origini dati ed eseguono trasformazioni in Power Query Online. Possono
quindi essere usati nelle pipeline di dati per inserire dati in un
lakehouse o in un altro archivio analitico oppure per definire un set di
dati per un report di Power BI.

Questo lab è progettato per introdurre i diversi elementi dei flussi di
dati (Gen2) e non per creare una soluzione complessa che può esistere in
un'azienda.

**Obiettivi**:

- Creare un'area di lavoro in Microsoft Fabric con la versione di
  valutazione di Fabric abilitata.

- Stabilire un ambiente lakehouse e carica i file di dati per l'analisi.

- Generare un notebook per l'esplorazione e l'analisi interattiva dei
  dati.

- Caricare i dati in un frame di dati per un'ulteriore elaborazione e
  visualizzazione.

- Applicare trasformazioni ai dati usando PySpark.

- Salvare e partizionare i dati trasformati per ottimizzare l'esecuzione
  di query.

- Creare una tabella nel metastore Spark per la gestione dei dati
  strutturati

- Salvare DataFrame come tabella delta gestita denominata "salesorders".

- Salvare DataFrame come tabella delta esterna denominata
  "external_salesorder" con un percorso specificato.

- Descrivere e confrontare le proprietà delle tabelle gestite ed
  esterne.

- Eseguire query SQL su tabelle per l'analisi e la creazione di report.

- Visualizzare i dati utilizzando librerie Python come matplotlib e
  seaborn.

- Stabilire un data lakehouse nell'esperienza di Data Engineering e
  acquisisci i dati rilevanti per l'analisi successiva.

- Definire un flusso di dati per l'estrazione, la trasformazione e il
  caricamento dei dati nella lakehouse.

- Configurare le destinazioni dei dati all'interno di Power Query per
  archiviare i dati trasformati nel lakehouse.

- Incorporare il flusso di dati in una pipeline per consentire
  l'elaborazione e l'inserimento pianificati dei dati.

- Rimuovere l'area di lavoro e gli elementi associati per concludere
  l'esercizio.

# Esercizio 1: Creare un'area di lavoro, una lakehouse, un notebook e caricare i dati nel frame di dati 

## Attività 1: Creare un'area di lavoro 

Prima di utilizzare i dati in Fabric, creare un'area di lavoro con la
versione di valutazione di Fabric abilitata.

1.  Aprire il browser, andare alla barra degli indirizzi e digitare o
    incollare il seguente URL: <https://app.fabric.microsoft.com/>
    quindi premi il pulsante **Enter**.

> **Nota**: se si viene indirizzati alla home page di Microsoft Fabric,
> saltare i passaggi da \#2 a \#4.
>
> ![A screenshot of a computer AI-generated content may be
> incorrect.](./media/image1.png)

2.  Nella finestra di **Microsoft Fabric**, inserire le tue credenziali
    e fare clic sul pulsante **Submit**.

> ![A close up of a white and green object AI-generated content may be
> incorrect.](./media/image2.png)

3.  Quindi, nella finestra **Microsoft,** inserire la password e fare
    clic sul pulsante **Sign in.**

> ![Una schermata di accesso con una casella rossa e un testo blu
> Descrizione generata automaticamente](./media/image3.png)

4.  In finestra **Stay signed in?,** fare clic sul pulsante **Yes**.

> ![Uno screenshot di un errore del computer Descrizione generata
> automaticamente](./media/image4.png)

5.  Home page dell'infrastruttura, selezionare riquadro **+New
    workspace**.

> ![Uno screenshot di un computer I contenuti generati dall'intelligenza
> artificiale potrebbero non essere corretti.](./media/image5.png)

6.  Nella scheda **Create a workspace**, inserire i seguenti dettagli e
    fare clic sul pulsante **Apply**.

[TABLE]

> ![Uno screenshot di un computer Descrizione generata
> automaticamente](./media/image6.png)
>
> ![A screenshot of a computer AI-generated content may be
> incorrect.](./media/image7.png)

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image8.png)

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image9.png)

7.  Attendere il completamento della distribuzione. Ci vogliono 2-3
    minuti per completarlo. Quando si apre la nuova area di lavoro,
    dovrebbe essere vuota.

## Attività 2: Creare una lakehouse e caricare i file

Ora che si dispone di un'area di lavoro, è il momento di passare
all'esperienza di *Data engineering* nel portale e creare un data
lakehouse per i file di dati che si intende analizzare.

1.  Creare una nuova Eventhouse cliccando sul pulsante ** +New item**
    nella barra di navigazione.

![Uno screenshot di un browser I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image10.png)

2.  Fare clic sul riquadro "**Lakehouse**".

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image11.png)

3.  Nella finestra di dialogo  **New lakehouse**, inserire
    **+++Fabric_lakehouse+++** nel campo **Name**, fare clic sul
    pulsante **Create** e aprire la nuova lakehouse.

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image12.png)

4.  Dopo circa un minuto, verrà creata una nuova lakehouse vuota. È
    necessario inserire alcuni dati nel data lakehouse per l'analisi.

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image13.png)

5.  Verrà visualizzata una notifica che indica **Successfully created
    SQL endpoint**.

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image14.png)

6.  Nella sezione **Explorer**, sotto la **fabric_lakehouse**, passa il
    mouse accanto alla **Files folder**, quindi fare clic sui puntini di
    sospensione orizzontali **(...)** menù. Navigare e fare clic su
    **Upload**, quindi fare clic su **Upload folder** come mostrato
    nell'immagine sottostante.

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image15.png)

7.  Nel riquadro **Upload folder** che appare sul lato destro,
    selezionare **folder icon** sotto **Files/**, quindi andare a
    **C:\LabFiles**, quindi seleziona la cartella degli **orders** e
    fare clic sul pulsante **Upload**.

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image16.png)

8.  Nel caso, **Upload 3 files to this site?** viene visualizzata la
    finestra di dialogo, quindi fare clic sul pulsante **Upload**.

![A blue square with red square and black text AI-generated content may
be incorrect.](./media/image17.png)

9.  Nel riquadro **Upload folder**, fare clic sul pulsante **Upload**.

> ![A screenshot of a computer AI-generated content may be
> incorrect.](./media/image18.png)

10. Dopo che i file sono stati caricati**, close** il riquadro **Upload
    folder**.

> ![A screenshot of a computer AI-generated content may be
> incorrect.](./media/image19.png)

11. Espandere **File** e selezionare la cartella degli **orders **e
    verificare che i file CSV siano stati caricati.

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image20.png)

## Attività 3: Creare un notebook

Per usare i dati in Apache Spark, è possibile creare un *notebook*. I
Notebooks forniscono un ambiente interattivo in cui è possibile scrivere
ed eseguire codice (in più lingue) e aggiungere note per documentarlo.

1.  Nella **Home** page, durante la visualizzazione del contenuto della
    cartella degli **orders ** nel datalake, nel menu **Open notebook**,
    selezionare **New notebook.**

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image21.png)

2.  Dopo alcuni secondi, si aprirà un nuovo notebook contenente una
    singola *cella*. I Notebooks sono costituiti da una o più celle che
    possono contenere *codice* o *markdown* (testo formattato).

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image22.png)

3.  Selezionare la prima cella (che attualmente è una cella di
    *codice*), quindi nella barra degli strumenti dinamica in alto a
    destra, usare il pulsante **M↓** per **convertire la cella in una
    cella markdown**.

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image23.png)

4.  Quando la cella si trasforma in una cella markdown, viene eseguito
    il rendering del testo in essa contenuto.

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image24.png)

5.  Utilizzare il **🖉** pulsante (Modifica) per passare la cella alla
    modalità di modifica, sostituire tutto il testo, quindi modificare
    il markdown come segue:

> CodeCopy
>
> \# Sales order data exploration 
>
> Utilizzare il codice in questo notebook per esplorare i dati degli
> ordini di vendita.

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image25.png)

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image26.png)

6.  Fare clic in un punto qualsiasi del notebook all'esterno della cella
    per interrompere la modifica e visualizzare il markdown di cui è
    stato eseguito il rendering.

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image27.png)

## Attività 4: Caricare i dati in un dataframe 

A questo punto è possibile eseguire il codice che carica i dati in un
*dataframe*. I dataframe in Spark sono simili ai dataframe Pandas in
Python e forniscono una struttura comune per lavorare con i dati in
righe e colonne.

**Nota**: Spark supporta più linguaggi di programmazione, tra cui Scala,
Java e altri. In questo esercizio si userà *PySpark*, che è una variante
di Python ottimizzata per Spark. PySpark è uno dei linguaggi più
comunemente usati in Spark ed è la lingua predefinita nei notebook
Fabric.

1.  Con il notebook visibile, espandere l'elenco **Files** e selezionare
    la cartella degli **orders  **in modo che i file CSV siano elencati
    accanto all'editor del notebook.

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image28.png)

2.  Ora, tuttavia, il mouse per 2019.csv file. Fare clic sulle ellissi
    orizzontali **(...)** accanto a 2019.csv. Navigare e fare clic su
    **Load data**, quindi seleziona **Spark**. Al notebook verrà
    aggiunta una nuova cella di codice contenente il codice seguente:

> CodeCopy 
>
> df =
> spark.read.format("csv").option("header","true").load("Files/orders/2019.csv") 
>
> \# df now is a Spark DataFrame containing CSV data from
> "Files/orders/2019.csv". 
>
> display(df) 

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image29.png)

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image30.png)

**Suggerimento**: puoi nascondere i riquadri di Lakehouse Explorer a
sinistra utilizzando le loro **icone «**. Facendo quindi ti aiuterà a
concentrarti sul notebook.

3.  Usare il pulsante **▷ Run cell ** a sinistra della cella per
    eseguirla.

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image31.png)

**Nota**: poiché è la prima volta che esegui un codice Spark, è
necessario avviare una sessione Spark. Ciò significa che il
completamento della prima esecuzione della sessione può richiedere circa
un minuto. Le corse successive saranno più veloci.

4.  Al termine del comando della cella, esaminare l'output sotto la
    cella, che dovrebbe essere simile al seguente:

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image32.png)

5.  L'output mostra le righe e le colonne di dati del file 2019.csv.
    Tuttavia, tieni presente che le intestazioni di colonna non hanno un
    aspetto corretto. Il codice predefinito utilizzato per caricare i
    dati in un frame di dati presuppone che il file CSV includa i nomi
    delle colonne nella prima riga, ma in questo caso il file CSV
    include solo i dati senza informazioni di intestazione.

6.  Modificare il codice per impostare l'opzione di **header ** su
    **false**. Sostituire tutto il codice nella **cella** con il
    seguente codice e fare clic sul pulsante **▷ Run cell** e rivedere
    l'output

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
> ![A screenshot of a computer AI-generated content may be
> incorrect.](./media/image33.png)

7.  Ora il dataframe include correttamente la prima riga come valori di
    dati, ma i nomi delle colonne sono generati automaticamente e non
    sono molto utili. Per dare un senso ai dati, è necessario definire
    in modo esplicito lo schema e il tipo di dati corretti per i valori
    dei dati nel file.

8.  Sostituire tutto il codice nella **cella** con il seguente codice e
    fare clic sul pulsante **▷ Run cell** e rivedere l'output

> CodeCopy
>
> from pyspark.sql.types import \* 
>
>  
>
> orderSchema = StructType(\[ 
>
>     StructField("SalesOrderNumber", StringType()), 
>
>     StructField("SalesOrderLineNumber", IntegerType()), 
>
>     StructField("OrderDate", DateType()), 
>
>     StructField("CustomerName", StringType()), 
>
>     StructField("Email", StringType()), 
>
>     StructField("Item", StringType()), 
>
>     StructField("Quantity", IntegerType()), 
>
>     StructField("UnitPrice", FloatType()), 
>
>     StructField("Tax", FloatType()) 
>
>     \]) 
>
>  
>
> df =
> spark.read.format("csv").schema(orderSchema).load("Files/orders/2019.csv") 
>
> display(df) 

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image34.png)

> ![A screenshot of a computer AI-generated content may be
> incorrect.](./media/image35.png)

9.  Ora il dataframe include i nomi di colonna corretti (oltre
    all'**Index**, che è una colonna incorporata in tutti i dataframe in
    base alla posizione ordinale di ogni riga). I tipi di dati delle
    colonne vengono specificati utilizzando un set standard di tipi
    definiti nella libreria Spark SQL, importati all'inizio della cella.

10. Verificare che le modifiche siano state applicate ai dati
    visualizzando il dataframe.

11. Utilizzare l'icona **+ Code ** sotto l'output della cella per
    aggiungere una nuova cella di codice al notebook e immettere il
    codice seguente. Fare clic sul pulsante **▷ Run cell** e rivedere
    l'output

> CodeCopy
>
> display(df)
>
> ![Uno screenshot di un computer Descrizione generata
> automaticamente](./media/image36.png)

12. Il dataframe include solo i dati del file **2019.csv**. Modificare
    il codice in modo che il percorso del file utilizzi un carattere
    jolly \* per leggere i dati dell'ordine cliente da tutti i file
    nella cartella degli **orders**

13. Utilizzare l'icona **+ Code ** sotto l'output della cella per
    aggiungere una nuova cella di codice al notebook e immettere il
    codice seguente.

CodeCopy

> from pyspark.sql.types import \* 
>
>  
>
> orderSchema = StructType(\[ 
>
>     StructField("SalesOrderNumber", StringType()), 
>
>     StructField("SalesOrderLineNumber", IntegerType()), 
>
>     StructField("OrderDate", DateType()), 
>
>     StructField("CustomerName", StringType()), 
>
>     StructField("Email", StringType()), 
>
>     StructField("Item", StringType()), 
>
>     StructField("Quantity", IntegerType()), 
>
>     StructField("UnitPrice", FloatType()), 
>
>     StructField("Tax", FloatType()) 
>
>     \]) 
>
>  
>
> df =
> spark.read.format("csv").schema(orderSchema).load("Files/orders/\*.csv") 
>
> display(df) 
>
> \])
>
> df =
> spark.read.format("csv").schema(orderSchema).load("File/ordini/\*.csv")
>
> Schermo (DF)

![](./media/image37.png)

14. Eseguire la cella di codice modificata ed esamina l'output, che ora
    dovrebbe includere le vendite per il 2019, 2020 e 2021.

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image38.png)

**Nota**: viene visualizzato solo un sottoinsieme delle righe, quindi
potrebbe non essere possibile visualizzare esempi di tutti gli anni.

# Esercizio 2: Esplorare i dati in un dataframe

L'oggetto dataframe include un'ampia gamma di funzioni che è possibile
utilizzare per filtrare, raggruppare e modificare in altro modo i dati
in esso contenuti.

## Attività 1: Filtrare un dataframe 

1.  Utilizzare l'icona **+ Code ** sotto l'output della cella per
    aggiungere una nuova cella di codice al notebook e immettere il
    codice seguente.

**CodeCopy**

> customers = df\['CustomerName', 'Email'\] 
>
> print(customers.count()) 
>
> print(customers.distinct().count()) 
>
> display(customers.distinct()) 
>
> ![A screenshot of a computer AI-generated content may be
> incorrect.](./media/image39.png)

2.  **Run** la nuova cella di codice ed esaminare i risultati. Osservare
    i seguenti dettagli:

    - Quando si esegue un'operazione su un frame di dati, il risultato è
      un nuovo frame di dati (in questo caso, viene creato un nuovo
      dataframe **customers** selezionando un sottoinsieme specifico di
      colonne dal dataframe df**)**

    - I dataframe forniscono funzioni come **count** e **distinct** che
      possono essere utilizzate per riepilogare e filtrare i dati che
      contengono.

    - Il dataframe\['Field1', 'Field2', ...\] syntax è un modo
      abbreviato per definire un sottoinsieme di colonne. Puoi anche
      utilizzare il metodo **select**, in modo che la prima riga del
      codice sopra possa essere scritta come customers =
      df.select("CustomerName", "Email")

> ![A screenshot of a computer AI-generated content may be
> incorrect.](./media/image40.png)

3.  Modificare il codice, sostituire tutto il codice nella **cella** con
    il seguente codice e fare clic sul pulsante **▷ Run cell** come
    segue:

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

4.  **Run** il codice modificato per visualizzare i clienti che hanno
    acquistato il ***prodotto* Road-250 Red, 52**. Si noti che è
    possibile " **chain**" più funzioni in modo che l'output di una
    funzione diventi l'input per la successiva: in questo caso, il
    dataframe creato dal metodo **select** è il dataframe di origine per
    il metodo **where** utilizzato per applicare i criteri di
    filtraggio.

> ![A screenshot of a computer AI-generated content may be
> incorrect.](./media/image41.png)

## Attività 2: Aggregare e raggruppare i dati in un dataframe

1.  Fare clic su **+ Code** e copiare e incollare il codice sottostante,
    quindi fare clic sul pulsante **Run cell**.

CodeCopy

> productSales = df.select("Item", "Quantity").groupBy("Item").sum() 
>
> display(productSales) 
>
> ![A screenshot of a computer AI-generated content may be
> incorrect.](./media/image42.png)

2.  Si noti che i risultati mostrano la somma delle quantità dell'ordine
    raggruppate per prodotto. Il metodo **groupBy** raggruppa le righe
    in base a *Item*, e la successiva funzione di aggregazione **sum**
    viene applicata a tutte le colonne numeriche rimanenti (in questo
    caso**,** Quantity*)*

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image43.png)

3.  Fare clic su **+ Code** e copiare e incollare il codice sottostante,
    quindi fare clic sul pulsante **Run cell**.

> **CodeCopy**
>
> from pyspark.sql.functions import \* 
>
>  
>
> yearlySales =
> df.select(year("OrderDate").alias("Year")).groupBy("Year").count().orderBy("Year") 
>
> display(yearlySales) 

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image44.png)

4.  Si noti che i risultati mostrano il numero di ordini cliente
    all'anno. Si noti che il metodo **select** include una funzione SQL
    **year** per estrarre il componente year del campo *OrderDate*
    (motivo per cui il codice include un' istruzione **import** per
    importare funzioni dalla libreria Spark SQL). Utilizza quindi un
    metodo **alias** utilizzato per assegnare un nome di colonna al
    valore dell'anno estratto. I dati vengono quindi raggruppati in base
    alla colonna Year derivata e viene calcolato il conteggio delle
    righe in ciascun gruppo prima di utilizzare il metodo **orderBy**
    per ordinare il frame di dati risultante.

![](./media/image45.png)

# Esercizio 3: Utilizzo di Spark per trasformare i file di dati

Un'attività comune per gli ingegneri dei dati consiste nell'inserire i
dati in un formato o una struttura particolare e trasformarli per
un'ulteriore elaborazione o analisi a valle.

## Attività 1: Utilizzare i metodi e le funzioni del dataframe per trasformare i dati

1.  Cliccare su + Code e copiare e incollare il codice sottostante

**CodeCopy**

> from pyspark.sql.functions import \* 
>
>  
>
> \## Create Year and Month columns 
>
> transformed_df = df.withColumn("Year",
> year(col("OrderDate"))).withColumn("Month", month(col("OrderDate"))) 
>
>  
>
> \# Create the new FirstName and LastName fields 
>
> transformed_df = transformed_df.withColumn("FirstName",
> split(col("CustomerName"), " ").getItem(0)).withColumn("LastName",
> split(col("CustomerName"), " ").getItem(1)) 
>
>  
>
> \# Filter and reorder columns 
>
> transformed_df = transformed_df\["SalesOrderNumber",
> "SalesOrderLineNumber", "OrderDate", "Year", "Month", "FirstName",
> "LastName", "Email", "Item", "Quantity", "UnitPrice", "Tax"\] 
>
>  
>
> \# Display the first five orders 
>
> display(transformed_df.limit(5)) 

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image46.png)

2.  **Run** il codice per creare un nuovo dataframe dai dati dell'ordine
    originale con le seguenti trasformazioni:

    - Aggiungere le colonne **Anno** e **Mese** in base alla colonna
      **OrderDate**.

    - Aggiungere le colonne **FirstName** e **LastName** in base alla
      colonna **CustomerName**.

    - Filtrare e riordinare le colonne, rimuovendo la colonna
      **CustomerName**.

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image47.png)

3.  Esaminare l'output e verificare che siano state apportate le
    trasformazioni ai dati.

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image48.png)

È possibile usare tutta la potenza della libreria Spark SQL per
trasformare i dati filtrando le righe, derivando, rimuovendo,
rinominando le colonne e applicando qualsiasi altra modifica dei dati
necessaria.

**Suggerimento**: Consultare la documentazione del [*dataframe di
Spark*](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html)
per saperne di più sui metodi dell'oggetto Dataframe.

## Attività 2: Salvare i dati trasformati

1.  **Aggiungere una nuova cella** con il codice seguente per salvare il
    frame di dati trasformato in formato Parquet (Sovrascrittura dei
    dati se già esistenti). **Run** la cella e attendi il messaggio che
    i dati sono stati salvati.

> CodeCopy
>
> transformed_df.write.mode("overwrite").parquet('Files/transformed_data/orders') 
>
> print ("Transformed data saved!") 
>
> **Nota**: in genere, *il formato Parquet* è preferibile per i file di
> dati che verranno usati per ulteriori analisi o per l'inserimento in
> un archivio analitico. Parquet è un formato molto efficiente
> supportato dalla maggior parte dei sistemi di analisi dei dati su
> larga scala. In effetti, a volte l'esigenza di trasformazione dei dati
> può essere semplicemente quella di convertire i dati da un altro
> formato (come CSV) a Parquet!

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image49.png)

![A screen shot of a computer code AI-generated content may be
incorrect.](./media/image50.png)

2.  Quindi, nel riquadro **Lakehouse Explorer** a sinistra, nella
    finestra **...** per il nodo **Files**, selezionare **Refresh**.

> ![A screenshot of a computer AI-generated content may be
> incorrect.](./media/image51.png)

3.  Fare clic sulla cartella **transformed_data** per verificare che
    contenga una nuova cartella denominata **orders**, che a sua volta
    contiene uno o più **Parquet files**.

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image52.png)

4.  Fare clic su **+ Code** seguente per caricare un nuovo dataframe dai
    file parquet nella cartella **transformed_data -\> orders**:

> **CodeCopy**
>
> orders_df =
> spark.read.format("parquet").load("Files/transformed_data/orders") 
>
> display(orders_df) 
>
> ![A screenshot of a computer AI-generated content may be
> incorrect.](./media/image53.png)

5.  **Run** la cella e verificare che i risultati mostrino i dati
    dell'ordine caricati dai file del parquet.

> ![A screenshot of a computer AI-generated content may be
> incorrect.](./media/image54.png)

## Attività 3: Salvare i dati in file partizionati

1.  Aggiungere una nuova cella, fare clic su **+ Code **con il seguente
    codice; che salva il dataframe, partizionando i dati per **Year **e
    **Month**. **Run** la cella e attendi il messaggio che i dati sono
    stati salvati

> CodeCopy
>
> orders_df.write.partitionBy("Year","Month").mode("overwrite").parquet("Files/partitioned_data") 
>
> print ("Transformed data saved!") 
>
> ![A screenshot of a computer AI-generated content may be
> incorrect.](./media/image55.png)
>
> ![A screenshot of a computer AI-generated content may be
> incorrect.](./media/image56.png)

2.  Quindi, nel riquadro **Lakehouse Explorer** a sinistra, nella
    finestra **...** per il nodo **Files**, selezionare **Refresh.**

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image57.png)

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image58.png)

3.  Espandere la cartella **partitioned_orders** per verificare che
    contenga una gerarchia di cartelle denominate **Year=*xxxx***,
    ognuna contenente cartelle denominate **Month=*xxxx***. Ogni
    cartella mensile contiene un file parquet con gli ordini per quel
    mese.

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image59.png)

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image60.png)

> Il partizionamento dei file di dati è un modo comune per ottimizzare
> le prestazioni quando si gestiscono grandi volumi di dati. Questa
> tecnica può migliorare significativamente le prestazioni e
> semplificare il filtraggio dei dati.

4.  Aggiungere una nuova cella, cliccare su **+ Code** con il seguente
    codice per caricare un nuovo dataframe dal file **orders.parquet**:

> CodeCopy
>
> orders_2021_df =
> spark.read.format("parquet").load("Files/partitioned_data/Year=2021/Month=\*") 
>
> display(orders_2021_df) 

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image61.png)

5.  **Run** la cella e verifica che i risultati mostrino i dati degli
    ordini per le vendite nel 2021. Si noti che le colonne di
    partizionamento specificate nel percorso (**Year ** e **Month**) non
    sono incluse nel dataframe.

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image62.png)

# **Esercizio 3: Utilizzo di tabelle e SQL**

Come hai visto, i metodi nativi dell'oggetto dataframe ti consentono di
interrogare e analizzare i dati da un file in modo abbastanza efficace.
Tuttavia, molti analisti di dati sono più a loro agio nell'uso di
tabelle su cui possono eseguire query utilizzando la sintassi SQL. Spark
fornisce un *metastore* in cui è possibile definire tabelle relazionali.
La libreria Spark SQL che fornisce l'oggetto dataframe supporta anche
l'uso di istruzioni SQL per eseguire query sulle tabelle nel metastore.
Utilizzando queste funzionalità di Spark, è possibile combinare la
flessibilità di un data lake con lo schema di dati strutturati e le
query basate su SQL di un data warehouse relazionale, da cui il termine
"data lakehouse".

## Attività 1: Creare una tabella gestita

Le tabelle in un metastore Spark sono astrazioni relazionali sui file
nel data lake. Le tabelle possono essere *gestite* (nel qual caso i file
vengono gestiti dal metastore) o *esterne* (nel qual caso la tabella fa
riferimento a un percorso di file nel data lake gestito
indipendentemente dal metastore).

1.  Aggiungere un nuovo codice, fare clic sulla cella **+ Code** al
    notebook e inserire il seguente codice, che salva il dataframe dei
    dati dell'ordine di vendita come una tabella denominata
    **salesorders**:

> CodeCopy
>
> \# Create a new table 
>
> df.write.format("delta").saveAsTable("salesorders") 
>
>  
>
> \# Get the table description 
>
> spark.sql("DESCRIBE EXTENDED salesorders").show(truncate=False) 

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image63.png)

**Nota**: vale la pena notare un paio di cose su questo esempio.
Innanzitutto, non viene fornito alcun percorso esplicito, quindi i file
per la tabella saranno gestiti dal metastore. In secondo luogo, la
tabella viene salvata in formato **delta**. È possibile creare tabelle
basate su più formati di file (tra cui CSV, Parquet, Avro e altri), ma
*delta lake* è una tecnologia Spark che aggiunge funzionalità di
database relazionali alle tabelle, tra cui il supporto per le
transazioni, il controllo delle versioni delle righe e altre funzioni
utili. La creazione di tabelle in formato delta è preferibile per i data
lakehouse in Fabric.

2.  **Run** la cella di codice ed esaminare l'output, che descrive la
    definizione della nuova tabella.

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image64.png)

3.  Nel riquadro **Lakehouse explorer**, nel **...** per la cartella
    **Tables**, selezionare **Refresh.**

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image65.png)

4.  Espandere quindi il nodo **Tables ** e verificare che la tabella
    **salesorders** sia stata creata.

> ![Uno screenshot di un computer Descrizione generata
> automaticamente](./media/image66.png)

5.  Passa il mouse accanto alla tabella degli **salesorders**, quindi
    fare clic sui puntini di sospensione orizzontali (...). Navigare e
    fare clic su **Load data**, quindi selezionare **Spark**.

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image67.png)

6.  Fare clic sul pulsante **▷ Run cell** e che utilizza la libreria
    Spark SQL per incorporare una query SQL rispetto alla tabella
    **salesorder** nel codice PySpark e caricare i risultati della query
    in un frame di dati.

> CodeCopy
>
> df = spark.sql("SELECT \* FROM \[your_lakehouse\].salesorders LIMIT
> 1000") 
>
> display(df) 

![Uno screenshot di un programma per computer Descrizione generata
automaticamente](./media/image68.png)

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image69.png)

## Attività 2: Creare una tabella esterna

È inoltre possibile creare tabelle *esterne* per le quali i metadati
dello schema sono definiti nel metastore per il lakehouse, ma i file di
dati vengono archiviati in una posizione esterna.

1.  Sotto i risultati restituiti dalla prima cella di codice, usare il
    pulsante **+ Code ** per aggiungere una nuova cella di codice, se
    non ne esiste già una. Quindi inserisci il seguente codice nella
    nuova cella.

CodeCopy

> df.write.format("delta").saveAsTable("external_salesorder",
> path="\<abfs_path\>/external_salesorder") 

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image70.png)

2.  Nel riquadro **Lakehouse explorer**, nel **...** per la cartella
    **Files**, selezionare **Copy ABFS path** nel blocco note.

> The ABFS path is the fully qualified path to the **Files** folder in
> the OneLake storage for your lakehouse - similar to this:

abfss://dp_Fabric29@onelake.dfs.fabric.microsoft.com/Fabric_lakehouse.Lakehouse/Files/external_salesorder

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image71.png)

3.  A questo punto, spostarsi nella cella del codice, sostituire
    **\<abfs_path\>** con il **percorso** copiato nel blocco note in
    modo che il codice salvi il frame di dati come tabella esterna con i
    file di dati in una cartella denominata **external_salesorder** nel
    percorso della cartella **Files**. Il percorso completo dovrebbe
    essere simile al seguente

abfss://dp_Fabric29@onelake.dfs.fabric.microsoft.com/Fabric_lakehouse.Lakehouse/Files/external_salesorder

4.  Usa il pulsante **▷ (*Run cell*)** a sinistra della cella per
    eseguirla.

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image72.png)

5.  Nel riquadro **Lakehouse explorer**, nel **...** per la cartella
    **Tables **, selezionare l'icona **Refresh**.

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image73.png)

6.  Espandere quindi il nodo **Tables **e verificare che la tabella
    **external_salesorder** sia stata creata.

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image74.png)

7.  Nel riquadro **Lakehouse explorer**, nel **...** per la cartella
    **Files**, selezionare **Refresh**.

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image75.png)

8.  Espandere quindi il nodo **File** e verificare che sia stata creata
    la cartella **external_salesorder** per i file di dati della
    tabella.

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image76.png)

## Attività 3: Confronto tra tabelle gestite ed esterne

Di seguito vengono illustrate le differenze tra tabelle gestite ed
esterne.

1.  Sotto i risultati restituiti dalla cella di codice, utilizzare il
    pulsante **+ Code **per aggiungere una nuova cella di codice. Copia
    il codice seguente nella cella Codice e usa il pulsante **▷ (*Run
    cell*)** a sinistra della cella per eseguirlo.

> SqlCopy
>
> %%sql 
>
>  
>
> DESCRIBE FORMATTED salesorders; 

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image77.png)

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image78.png)

2.  Nei risultati visualizzare la proprietà **Location** per la tabella,
    che deve essere un percorso per l'archiviazione OneLake per la
    lakehouse che termina con **/Tables/salesorders** (potrebbe essere
    necessario allargare la colonna **Data type** per visualizzare il
    percorso completo).

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image79.png)

3.  Modificare il commando **DESCRIBE** per visualizzare i dettagli
    della tabella **external_saleorder** come illustrato di seguito.

4.  Sotto i risultati restituiti dalla cella di codice, utilizzare il
    pulsante **+ Code** per aggiungere una nuova cella di codice.
    Copiare il codice seguente e usa il pulsante **▷ (*Run cell*)** a
    sinistra della cella per eseguirlo.

> SqlCopy
>
> %%sql 
>
>  
>
> DESCRIBE FORMATTED external_salesorder; 

![Uno screenshot di un'e-mail Descrizione generata
automaticamente](./media/image80.png)

5.  Nei risultati visualizzare la proprietà **Location** per la tabella,
    che deve essere un percorso per l'archiviazione OneLake per la
    lakehouse che termina con **/Files/external_saleorder** (potrebbe
    essere necessario ampliare la colonna **Data type** per visualizzare
    il percorso completo).

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image81.png)

## Attività 4: Eseguire codice SQL in una cella

Sebbene sia utile essere in grado di incorporare istruzioni SQL in una
cella contenente codice PySpark, gli analisti di dati spesso vogliono
semplicemente lavorare direttamente in SQL.

1.  Fare clic sulla cella **+ Code** del taccuino e inserire il seguente
    codice. Fare clic sul pulsante **▷ Run cell** e rivedere i
    risultati. Osserva che:

    - La riga %%sql all'inizio della cella (denominata *magia*) indica
      che il runtime del linguaggio Spark SQL deve essere usato per
      eseguire il codice in questa cella anziché PySpark.

    - Il codice SQL fa riferimento alla tabella **salesorders** creata
      in precedenza.

    - L'output della query SQL viene visualizzato automaticamente come
      risultato sotto la cella

> SqlCopy
>
> %%sql 
>
> SELECT YEAR(OrderDate) AS OrderYear, 
>
>        SUM((UnitPrice \* Quantity) + Tax) AS GrossRevenue 
>
> FROM salesorders 
>
> GROUP BY YEAR(OrderDate) 
>
> ORDER BY OrderYear; 

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image82.png)

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image83.png)

**Nota**: per ulteriori informazioni su Spark SQL e sui frame di dati,
vedere la documentazione di [*Spark
SQL*](https://spark.apache.org/docs/2.2.0/sql-programming-guide.html).

# Esercizio 4: Visualizzare i dati con Spark

Un'immagine vale proverbialmente più di mille parole e un grafico è
spesso meglio di mille righe di dati. Sebbene i notebook in Fabric
includano una visualizzazione grafico integrata per i dati visualizzati
da un frame di dati o da una query Spark SQL, non sono progettati per la
creazione di grafici completi. Tuttavia, è possibile utilizzare librerie
grafiche Python come **matplotlib** e **seaborn** per creare grafici dai
dati nei frame di dati.

## Attività 1: Visualizzare i risultati sotto forma di grafico

1.  Fare clic sulla cella **+ Code** del notebook e inserire il seguente
    codice. Fare clic sul pulsante **▷ Run cell** e osservare che
    restituisce i dati dalla visualizzazione degli
    **salesorders **creata in precedenza.

> SqlCopy
>
> %%sql 
>
> SELECT \* FROM salesorders 

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image84.png)

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image85.png)

2.  Nella sezione dei risultati sotto la cella, modificare l'opzione
    **View **da **Table** a **Chart**.

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image86.png)

3.  Utilizzare il pulsante **View options** in alto a destra del grafico
    per visualizzare il riquadro delle opzioni per il grafico. Quindi
    imposta le opzioni come segue e selezionare **Apply**:

- **Chart type**: Bar chart 

&nbsp;

- **Key**: Item 

&nbsp;

- **Values**: Quantity 

&nbsp;

- **Series Group**: *leave blank* 

&nbsp;

- **Aggregation**: Sum 

- **Stacked**: *non selezionato*

![Un codice a barre blu su sfondo bianco Descrizione generata
automaticamente](./media/image87.png)

![Uno screenshot di un grafico Descrizione generata
automaticamente](./media/image88.png)

4.  Verificare che il grafico sia simile al seguente

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image89.png)

## Compito 2: Iniziare con matplotlib

1.  Fare clic su **+ Code** e copiare e incollare il codice sottostante.
    **Run** il codice e osservare che restituisce un frame di dati Spark
    contenente le entrate annuali.

> CodeCopy
>
> sqlQuery = "SELECT CAST(YEAR(OrderDate) AS CHAR(4)) AS OrderYear, \\
>
>                 SUM((UnitPrice \* Quantity) + Tax) AS GrossRevenue \\
>
>             FROM salesorders \\
>
>             GROUP BY CAST(YEAR(OrderDate) AS CHAR(4)) \\
>
>             ORDER BY OrderYear" 
>
> df_spark = spark.sql(sqlQuery) 
>
> df_spark.show() 

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image90.png)

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image91.png)

2.  Per visualizzare i dati sotto forma di grafico, inizieremo
    utilizzando la libreria Python **matplotlib**. Questa libreria è la
    libreria di plottaggio principale su cui si basano molte altre e
    offre una grande flessibilità nella creazione di grafici.

3.  Fare clic su **+ Code** e copiare e incollare il codice sottostante.

**CodeCopy**

> from matplotlib import pyplot as plt 
>
>  
>
> \# matplotlib requires a Pandas dataframe, not a Spark one 
>
> df_sales = df_spark.toPandas() 
>
>  
>
> \# Create a bar plot of revenue by year 
>
> plt.bar(x=df_sales\['OrderYear'\], height=df_sales\['GrossRevenue'\]) 
>
>  
>
> \# Display the plot 
>
> plt.show() 

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image92.png)

5.  Fare clic sul pulsante **Run cell ** ed esaminare i risultati, che
    consistono in un istogramma con le entrate lorde totali per ogni
    anno. Si notino le seguenti caratteristiche del codice utilizzato
    per produrre questo grafico:

    - La libreria **matplotlib** richiede un dataframe *Pandas*, quindi
      è necessario convertire il frame di dati *Spark* restituito dalla
      query Spark SQL in questo formato.

    - Al centro della libreria **matplotlib** c'è l'oggetto **pyplot**.
      Questa è la base per la maggior parte delle funzionalità di
      stampa.

    - Le impostazioni predefinite consentono di ottenere un grafico
      utilizzabile, ma c'è un notevole margine di personalizzazione

![Uno screenshot dello schermo di un computer Descrizione generata
automaticamente](./media/image93.png)

6.  Modificare il codice per tracciare il grafico come segue,
    sostituisci tutto il codice nella **cella** con il seguente codice e
    fare clic sul pulsante **▷ Run cell** e rivedere l'output

> CodeCopy
>
> from matplotlib import pyplot as plt 
>
>  
>
> \# Clear the plot area 
>
> plt.clf() 
>
>  
>
> \# Create a bar plot of revenue by year 
>
> plt.bar(x=df_sales\['OrderYear'\], height=df_sales\['GrossRevenue'\],
> color='orange') 
>
>  
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
>  
>
> \# Show the figure 
>
> plt.show() 

![Uno screenshot di un grafico Descrizione generata
automaticamente](./media/image94.png)

7.  Il grafico ora include un po' più di informazioni. Un grafico è
    tecnicamente contenuto con una **figura**. Negli esempi precedenti,
    la figura è stata creata in modo implicito per l'utente; ma puoi
    crearlo in modo esplicito.

8.  Modificare il codice per tracciare il grafico come segue, sostituire
    tutto il codice nella **cella** con il seguente codice.

> CodeCopy
>
> from matplotlib import pyplot as plt 
>
>  
>
> \# Clear the plot area 
>
> plt.clf() 
>
>  
>
> \# Create a Figure 
>
> fig = plt.figure(figsize=(8,3)) 
>
>  
>
> \# Create a bar plot of revenue by year 
>
> plt.bar(x=df_sales\['OrderYear'\], height=df_sales\['GrossRevenue'\],
> color='orange') 
>
>  
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
>  
>
> \# Show the figure 
>
> plt.show() 

![Uno screenshot di un programma per computer Descrizione generata
automaticamente](./media/image95.png)

9.  **Re-run** la cella di codice e visualizzare i risultati. La figura
    determina la forma e le dimensioni del grafico.

> Una figura può contenere più sottotrame, ciascuna sul proprio *asse*.

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image96.png)

10. Modificare il codice per tracciare il grafico come indicato di
    seguito. **Re-run** la cella di codice e visualizzare i risultati.
    La figura contiene le sottotrame specificate nel codice.

> CodeCopy 
>
> from matplotlib import pyplot as plt 
>
>  
>
> \# Clear the plot area 
>
> plt.clf() 
>
>  
>
> \# Create a figure for 2 subplots (1 row, 2 columns) 
>
> fig, ax = plt.subplots(1, 2, figsize = (10,4)) 
>
>  
>
> \# Create a bar plot of revenue by year on the first axis 
>
> ax\[0\].bar(x=df_sales\['OrderYear'\],
> height=df_sales\['GrossRevenue'\], color='orange') 
>
> ax\[0\].set_title('Revenue by Year') 
>
>  
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
>  
>
> \# Add a title to the Figure 
>
> fig.suptitle('Sales Data') 
>
>  
>
> \# Show the figure 
>
> plt.show() 

![Uno screenshot di un programma per computer Descrizione generata
automaticamente](./media/image97.png)

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image98.png)

**Nota**: Per ulteriori informazioni sulla stampa con matplotlib,
consulta la [*documentazione di matplotlib*](https://matplotlib.org/).

## Compito 3: Utilizzare la libreria seaborn

Sebbene **matplotlib** consenta di creare grafici complessi di più tipi,
può richiedere un codice complesso per ottenere i migliori risultati.
Per questo motivo, nel corso degli anni, sono state costruite molte
nuove librerie sulla base di matplotlib per astrarne la complessità e
migliorarne le capacità. Una di queste biblioteche è quella **seaborn**.

1.  Fare clic su **+ Code** e copiare e incollare il codice sottostante.

CodeCopy

> import seaborn as sns 
>
>  
>
> \# Clear the plot area 
>
> plt.clf() 
>
>  
>
> \# Create a bar chart 
>
> ax = sns.barplot(x="OrderYear", y="GrossRevenue", data=df_sales) 
>
> plt.show() 

![Uno screenshot di un grafico Descrizione generata
automaticamente](./media/image99.png)

2.  **Run** il codice e osservare che viene visualizzato un grafico a
    barre utilizzando la libreria seaborn.

![Uno screenshot di un grafico Descrizione generata
automaticamente](./media/image100.png)

3.  **Modificare** il codice come indicato di seguito. **Run** il codice
    modificato e tieni presente che seaborn ti consente di impostare un
    tema di colori coerente per i tuoi grafici.

> CodeCopy 
>
> import seaborn as sns 
>
>  
>
> \# Clear the plot area 
>
> plt.clf() 
>
>  
>
> \# Set the visual theme for seaborn 
>
> sns.set_theme(style="whitegrid") 
>
>  
>
> \# Create a bar chart 
>
> ax = sns.barplot(x="OrderYear", y="GrossRevenue", data=df_sales) 
>
> plt.show() 
>
> ![A screenshot of a graph AI-generated content may be
> incorrect.](./media/image101.png)

4.  **Modificare** nuovamente il codice come indicato di seguito.
    **Run** il codice modificato per visualizzare i ricavi annuali come
    grafico a linee.

> CodeCopy 
>
> import seaborn as sns 
>
>  
>
> \# Clear the plot area 
>
> plt.clf() 
>
>  
>
> \# Create a bar chart 
>
> ax = sns.lineplot(x="OrderYear", y="GrossRevenue", data=df_sales) 
>
> plt.show() 

![A screen shot of a graph AI-generated content may be
incorrect.](./media/image102.png)

**Nota**: Per ulteriori informazioni sulla stampa con seaborn,
consultare la [*documentazione di
seaborn*](https://seaborn.pydata.org/index.html).

## Attività 4: Usare le tabelle differenziali per lo streaming dei dati

Delta Lake supporta lo streaming di dati. Le tabelle delta possono
essere un *sink* o un'*origine* per i flussi di dati creati usando l'API
Spark Structured Streaming. In questo esempio si userà una tabella delta
come sink per alcuni dati in streaming in uno scenario simulato di
Internet delle cose (IoT).

1.  Fare clic su **+ Code** e copiare e incollare il codice sottostante,
    quindi fare clic sul pulsante **Run cell**.

CodeCopy

> from notebookutils import mssparkutils 
>
> from pyspark.sql.types import \* 
>
> from pyspark.sql.functions import \* 
>
>  
>
> \# Create a folder 
>
> inputPath = 'Files/data/' 
>
> mssparkutils.fs.mkdirs(inputPath) 
>
>  
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
>  
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

![Uno screenshot di un programma per computer Descrizione generata
automaticamente](./media/image103.png)

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image104.png)

2.  Assicurati che il messaggio ***Source stream created…*** viene
    stampato. Il codice appena eseguito ha creato un'origine dati di
    streaming basata su una cartella in cui sono stati salvati alcuni
    dati, che rappresentano le letture da ipotetici dispositivi IoT.

3.  Fare clic su **+ Code** e copiare e incollare il codice sottostante,
    quindi fare clic sul pulsante **Run cell**.

> CodeCopy 
>
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

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image105.png)

4.  Questo codice scrive i dati del dispositivo di streaming in formato
    delta in una cartella denominata **iotdevicedata**. Poiché il
    percorso della cartella si trova nella cartella **Tables**, verrà
    creata automaticamente una tabella per tale cartella. Fare clic sui
    puntini di sospensione orizzontali accanto alla tabella, quindi fare
    clic su **Refresh**.

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image106.png)

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image107.png)

5.  Fare clic su **+ Code** e copiare e incollare il codice sottostante,
    quindi fare clic sul pulsante **Run cell**.

> SqlCopy 
>
> %%sql 
>
>  
>
> SELECT \* FROM IotDeviceData; 

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image108.png)

6.  Questo codice esegue una query sulla tabella **IotDeviceData**, che
    contiene i dati del dispositivo dall'origine di streaming.

7.  Fare clic su **+ Code** e copiare e incollare il codice sottostante,
    quindi fare clic sul pulsante **Run cell**.

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
>  
>
> mssparkutils.fs.put(inputPath + "more-data.txt", more_data, True)

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image109.png)

8.  Questo codice scrive più dati ipotetici del dispositivo nell'origine
    di streaming.

9.  Fare clic su **+ Code** e copiare e incollare il codice sottostante,
    quindi fare clic sul pulsante **Run cell**.

> SqlCopy 
>
> %%sql 
>
>  
>
> SELECT \* FROM IotDeviceData; 
>
> ![Uno screenshot di un computer Descrizione generata
> automaticamente](./media/image110.png)

10. Questo codice esegue nuovamente una query sulla tabella
    **IotDeviceData**, che ora dovrebbe includere i dati aggiuntivi
    aggiunti all'origine di streaming.

11. Fare clic su **+ Code** e copiare e incollare il codice sottostante,
    quindi fare clic sul pulsante **Run cell**.

> CodeCopy 
>
> deltastream.stop() 

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image111.png)

12. Questo codice interrompe il flusso.

## Attività 5: Salvare il notebook e terminare la sessione Spark

Dopo aver terminato di usare i dati, è possibile salvare il notebook con
un nome significativo e terminare la sessione Spark.

1.  Nella barra dei menu del notebook, utilizzare l'⚙️ icona
    **Settings **per visualizzare le impostazioni del notebook.

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image112.png)

2.  Impostare il **Name **del notebook su ++**Explore Sales
    Orders**+**+** e quindi chiudere il riquadro delle impostazioni.

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image113.png)

3.  Nel menu del notebook selezionare **Stop session** per terminare la
    sessione Spark.

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image114.png)

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image115.png)

# Esercizio 5: Creare un Dataflow (Gen2) in Microsoft Fabric

In Microsoft Fabric, i flussi di dati (Gen2) si connettono a varie
origini dati ed eseguono trasformazioni in Power Query Online. Possono
quindi essere usati nelle pipeline di dati per inserire dati in un
lakehouse o in un altro archivio analitico oppure per definire un set di
dati per un report di Power BI.

Questo esercizio è progettato per introdurre i diversi elementi dei
flussi di dati (Gen2) e non per creare una soluzione complessa che può
esistere in un'azienda

## Attività 1: Creare un Dataflow (Gen2) per inserire i dati

Ora che hai una lakehouse, devi inserire alcuni dati al suo interno. Un
modo per eseguire questa operazione consiste nel definire un flusso di
dati che incapsula un *processo di extract, transform, e load* (ETL).

1.  Ora, fare clic su **Fabric_lakehouse** nel riquadro di navigazione a
    sinistra.

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image116.png)

2.  Nella home page di **Fabric_lakehouse** fare clic sulla freccia a
    discesa nell'opzione **Get data** e selezionare ** New Dataflow
    Gen2.** Verrà aperto l'editor di Power Query per il nuovo flusso di
    dati.

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image117.png)

3.  Nel riquadro **Power Query**, nella scheda **Home**, fare clic su
    **Import from a Text/CSV file**.

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image118.png)

4.  Nel riquadro **Connect to data source**, in **Connection settings**,
    selezionare il pulsante di opzione **Link to file (Preview)**

- **Link to file**: *Selezionato*

- **File path or URL**:
  <https://raw.githubusercontent.com/MicrosoftLearning/dp-data/main/orders.csv>

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image119.png)

5.  Nel riquadro **Connect to data source**, in **Connection
    credentials,** immettere i dettagli seguenti e fare clic sul
    pulsante **Next**.

- **Connection**: Create new connection 

&nbsp;

- **data gateway**: (none) 

&nbsp;

- **Authentication kind**: Organizational account 

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image120.png)

6.  Nel riquadro **Preview file data**, fare clic su **Create **per
    creare l'origine dati. ![Uno screenshot di un computer Descrizione
    generata automaticamente](./media/image121.png)

7.  L'editor di **Power Query** mostra l'origine dati e un set iniziale
    di passaggi di query per formattare i dati.

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image122.png)

8.  Sulla barra multifunzione della barra degli strumenti, selezionare
    la scheda ** Add column**. Quindi, selezionare **Custom column.**

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image123.png) 

9.  Impostare il nome della nuova colonna su **MonthNo**, impostare il
    tipo di dati su **Whole Number**, quindi aggiungere la formula
    seguente:**Date.Month(\[OrderDate\])** in **Custom column formula**.
    Selezionare **OK.**

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image124.png)

10. Si noti come il passaggio per aggiungere la colonna personalizzata
    viene aggiunto alla query. La colonna risultante viene visualizzata
    nel riquadro dei dati.

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image125.png)

**Suggerimento:** Nel riquadro Query Settings sul lato destro, si noti
che i **Applied Steps** includono ogni passaggio di trasformazione. In
basso, puoi anche attivare il pulsante ** Diagram flow** per attivare il
diagramma visivo dei passaggi.

I passaggi possono essere spostati verso l'alto o verso il basso,
modificati selezionando l'icona a forma di ingranaggio ed è possibile
selezionare ogni passaggio per visualizzare le trasformazioni applicate
nel riquadro di anteprima.

Attività 2: Aggiungere la destinazione dei dati per il Dataflow

1.  Sulla barra multifunzione della barra degli strumenti di **Power
    Query**, selezionare la scheda **Home**. Quindi, nel menu a discesa
    **Data destination**, selezionare **Lakehouse** (se non è già
    selezionato).

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image126.png)

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image127.png)

**Nota:** se questa opzione è disattivata, è possibile che sia già
impostata una destinazione dati. Controllare la destinazione dei dati
nella parte inferiore del riquadro Impostazioni query sul lato destro
dell'editor di Power Query. Se una destinazione è già impostata, è
possibile modificarla utilizzando l'ingranaggio.

2.  Fare clic sull'icona **Settings** accanto all'opzione **Lakehouse**
    selezionata .

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image128.png)

3.  Nella finestra di dialogo **Connect to data destination,**
    selezionare **Edit connection.**

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image129.png)

4.  Nella finestra di dialogo **Connect to data destination,**
    selezionare **sign in** con l'account aziendale di Power BI per
    impostare l'identità usata dal flusso di dati per accedere al
    lakehouse.

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image130.png)

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image131.png)

5.  Nella finestra di dialogo **Connect to data destination**
    selezionare **Next**

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image132.png)

6.  Nella finestra di dialogo **Connect to data destination**
    selezionare **New table**. Fare clic sulla **cartella Lakehouse**,
    selezionare il tuo spazio di lavoro - **dp_FabricXX** e quindi
    selezionare la tua lakehouse, ad esempio **Fabric_lakehouse.**
    Quindi specificare il nome della tabella come **orders** e
    selezionare il pulsante **Next**.

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image133.png)

7.  Nella finestra di dialogo **Choose destination settings, in Use
    automatic settings off** e nel **Update method**, selezionare
    **Append**, quindi fare clic sul pulsante **Save settings**.

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image134.png)

8.  La destinazione **Lakehouse** è indicata come **icona** nella
    **query** nell'editor di Power Query.

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image135.png)

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image136.png)

9.  Selezionare **Publish** per pubblicare il flusso di dati. Attendere
    quindi che il flusso di dati **Dataflow 1** venga creato nell'area
    di lavoro.

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image137.png)

10. Una volta pubblicato, è possibile fare clic con il pulsante destro
    del mouse sul flusso di dati nell'area di lavoro, scegliere
    **Properties** e rinominare il flusso di dati.

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image138.png)

11. Nella finestra di dialogo **Dataflow1**, inserire il **Name** come
    **Gen2_Dataflow** e fare clic sul pulsante **Save**.

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image139.png)

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image140.png)

## Attività 3: Aggiungere un dataflow a una pipeline

È possibile includere un flusso di dati come attività in una pipeline.
Le pipeline vengono usate per orchestrare le attività di inserimento ed
elaborazione dei dati, consentendo di combinare i flussi di dati con
altri tipi di operazioni in un unico processo pianificato. Le pipeline
possono essere create in alcune esperienze diverse, tra cui l'esperienza
di Data Factory.

1.  Nella home page di Synapse Data Engineering , nel riquadro
    **dp_FabricXX** selezionare **+ New item** -\> **Data pipeline**

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image141.png)

2.  Nella finestra di dialogo **New pipeline**, immettere **Load data**
    nel campo **Name **, fare clic sul pulsante **Create** per aprire la
    nuova pipeline.

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image142.png)

3.  Viene visualizzato l'editor della pipeline.

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image143.png)

> **Suggerimento**: se la procedura guidata Copy Data si apre
> automaticamente, chiudila!

4.  Selezionare **Pipeline activity** e aggiungere un'attività
    **Dataflow**  alla pipeline.

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image144.png)

5.  Con la nuova attività **Dataflow1 **selezionata, nell'elenco a
    discesa **Dataflow **della scheda **Settings,** selezionare
    **Gen2_Dataflow** (il flusso di dati creato in precedenza)

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image145.png)

6.  Nella scheda **Home**, salvare la pipeline utilizzando l'**🖫** icona
    ***(*Save**).

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image146.png)

7.  Utilizzare il pulsante **▷ Run **per eseguire la pipeline e
    attendere il completamento. Potrebbero essere necessari alcuni
    minuti.

> ![Uno screenshot di un computer Descrizione generata
> automaticamente](./media/image147.png)
>
> ![Uno screenshot di un computer Descrizione generata
> automaticamente](./media/image148.png)

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image149.png)

8.  Nella barra dei menu sul bordo sinistro, selezionare l'area di
    lavoro, ad esempio **dp_FabricXX**.

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image150.png)

9.  Nel riquadro **Fabric_lakehouse,** selezionare il
    **Gen2_FabricLakehouse** di tipo Lakehouse.

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image151.png)

10. Nel riquadro **Explorer**, selezionare l'icona **...** per
    **Tables**, selezionare **refresh**. Quindi espandere **Tables ** e
    selezionare la tabella degli **orders**, che è stata creata dal
    dataflow.

![Uno screenshot di un computer Descrizione generata
automaticamente](./media/image152.png)

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image153.png)

**Suggerimento**: Usare il Power BI Desktop *Dataflows connector* per
connettersi direttamente alle trasformazioni dei dati eseguite con il
dataflow.

È inoltre possibile apportare ulteriori trasformazioni, pubblicare come
nuovo set di dati e distribuire con i destinatari previsti per i set di
dati specializzati.

## Attività 4: Pulire le risorse

In questo esercizio si è appreso come usare Spark per lavorare con i
dati in Microsoft Fabric. Se hai finito di esplorare la tua lakehouse,
puoi eliminare l'area di lavoro che hai creato per questo esercizio.

1.  Nella barra a sinistra, selezionare l'icona dell'area di lavoro per
    visualizzare tutti gli elementi che contiene.

> ![Uno screenshot di un computer Descrizione generata
> automaticamente](./media/image154.png)

2.  Nel **...** sulla barra degli strumenti, selezionare **Workspace
    settings**.

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image155.png)

3.  Selezionare **General** e fare clic su **Remove this workspace.**

![Uno screenshot delle impostazioni di un computer Descrizione generata
automaticamente](./media/image156.png)

4.  Nella finestra di dialogo **Delete workspace?**, fare clic sul
    pulsante **Delete**.

> ![Uno screenshot di un computer Descrizione generata
> automaticamente](./media/image157.png)
>
> ![Uno screenshot di un computer Descrizione generata
> automaticamente](./media/image158.png)

**Sommario**

Questo caso d'uso illustra il processo di utilizzo di Microsoft Fabric
all'interno di Power BI. Copre varie attività, tra cui l'impostazione di
un'area di lavoro, la creazione di una lakehouse, il caricamento e la
gestione di file di dati e l'uso di notebook per l'esplorazione dei
dati. I partecipanti impareranno come manipolare e trasformare i dati
utilizzando PySpark, creare visualizzazioni e salvare e partizionare i
dati per un'esecuzione efficiente di query.

In questo caso d'uso, i partecipanti si impegneranno in una serie di
attività incentrate sull'uso delle tabelle delta in Microsoft Fabric. Le
attività comprendono il caricamento e l'esplorazione dei dati, la
creazione di tabelle delta gestite ed esterne, il confronto delle loro
proprietà, il laboratorio introduce funzionalità SQL per la gestione dei
dati strutturati e fornisce approfondimenti sulla visualizzazione dei
dati utilizzando librerie Python come matplotlib e seaborn. Gli esercizi
hanno lo scopo di fornire una comprensione completa dell'utilizzo di
Microsoft Fabric per l'analisi dei dati e dell'incorporazione di tabelle
delta per lo streaming dei dati in un contesto IoT.

Questo caso d'uso ti guida attraverso il processo di configurazione di
un'area di lavoro Fabric, la creazione di un data lakehouse e
l'acquisizione dei dati per l'analisi. Viene illustrato come definire un
flusso di dati per gestire le operazioni ETL e configurare le
destinazioni dei dati per l'archiviazione dei dati trasformati. Inoltre,
si apprenderà come integrare il flusso di dati in una pipeline per
l'elaborazione automatizzata. Infine, ti verranno fornite le istruzioni
per ripulire le risorse una volta completato l'esercizio.

Questo lab fornisce le competenze essenziali per lavorare con Fabric,
consentendoti di creare e gestire aree di lavoro, creare data lakehouse
ed eseguire trasformazioni dei dati in modo efficiente. Incorporando i
flussi di dati nelle pipeline, imparerai come automatizzare le attività
di elaborazione dei dati, semplificando il flusso di lavoro e
migliorando la produttività in scenari reali. Le istruzioni per la
pulizia assicurano che non lasci risorse inutili, promuovendo un
approccio organizzato ed efficiente alla gestione dello spazio di
lavoro.
