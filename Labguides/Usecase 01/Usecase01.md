# Caso d'uso 1: Creazione di un Lakehouse, inserimento di dati di esempio e creazione di un report

**Introduzione**

Questo lab illustra uno scenario end-to-end dall'acquisizione dei dati
all'utilizzo dei dati. Ti aiuta a costruire una comprensione di base di
Fabric, comprese le diverse esperienze e il modo in cui si integrano,
nonché le esperienze di sviluppo professionale e cittadino che derivano
dal lavorare su questa piattaforma. Questo lab non è destinato a essere
un'architettura di riferimento, un elenco esaustivo di caratteristiche e
funzionalità o una raccomandazione di procedure consigliate specifiche.

Tradizionalmente, le organizzazioni hanno creato data warehouse moderni
per le loro esigenze di analisi dei dati transazionali e strutturati. E
data lakehouse per le esigenze di analisi dei dati dei big data
(semi/non strutturati). Questi due sistemi funzionavano in parallelo,
creando silos, duplicità dei dati e aumento del costo totale di
proprietà.

Fabric, con l'unificazione dell'archivio dati e la standardizzazione in
formato Delta Lake, consente di eliminare i silos, rimuovere la
duplicità dei dati e ridurre drasticamente il costo totale di proprietà.

Con la flessibilità offerta da Fabric, è possibile implementare
architetture lakehouse o data warehouse o combinarle insieme per
ottenere il meglio da entrambe con una semplice implementazione. In
questo tutorial, prenderai l'esempio di un'organizzazione di vendita al
dettaglio e costruirai la sua lakehouse dall'inizio alla fine. Utilizza
l'architettura a
[medaglione](https://learn.microsoft.com/en-us/azure/databricks/lakehouse/medallion)
in cui lo strato di bronzo contiene i dati grezzi, lo strato di argento
contiene i dati convalidati e deduplicati e lo strato di oro contiene
dati altamente raffinati. Puoi adottare lo stesso approccio per
implementare una lakehouse per qualsiasi organizzazione di qualsiasi
settore.

In questo lab viene illustrato il modo in cui uno sviluppatore della
società fittizia Wide World Importers del dominio di vendita al
dettaglio completa i passaggi seguenti.

**Obiettivi**:

1\. Accedere all'account Power BI e avviare una versione di valutazione
gratuita di Microsoft Fabric.

2\. Avviare la versione di valutazione di Microsoft Fabric (anteprima)
in Power BI.

3\. Configurare l'iscrizione a OneDrive per l'interfaccia di
amministrazione di Microsoft 365.

4\. Costruire e implementare una lakehouse end-to-end per
l'organizzazione, inclusa la creazione di un'area di lavoro Fabric e di
una lakehouse.

5\. Inserire i dati del campione nella lakehouse e prepararli per
un'ulteriore elaborazione.

6\. Trasformare e preparare i dati utilizzando i notebook Python/PySpark
e SQL.

7\. Creare tabelle aggregate aziendali utilizzando approcci diversi.

8\. Stabilire relazioni tra le tabelle per una reportistica senza
interruzioni.

9\. Creare un report di Power BI con visualizzazioni basate sui dati
preparati.

10\. Salvare e archiviare il report creato per riferimento e analisi
futuri.

## Esercizio 1: Impostazione dello scenario end-to-end di Lakehouse

### Attività 1: Accedere all'account Power BI e iscriversi alla [**versione di valutazione gratuita di Microsoft Fabric**](https://learn.microsoft.com/en-us/fabric/get-started/fabric-trial)

1.  Aprire il browser, andare alla barra degli indirizzi e digitare o
    incollare il seguente
    URL:+++[https://app.fabric.microsoft.com/+++,](https://app.fabric.microsoft.com/+++)
    quindi premi il pulsante **Enter**.

![](./media/image1.png)

2.  Nella finestra ** Microsoft Fabric**, inserire le tue credenziali
    **di Microsoft 365** e fare clic sul pulsante **Submit**.

![Un primo piano di un oggetto bianco e verde I contenuti generati
dall'intelligenza artificiale potrebbero non essere
corretti.](./media/image2.png)

3.  Quindi, nella finestra **Microsoft** inserire la password e fare
    clic sul pulsante **Sign in**.

![Una schermata di accesso con una casella rossa e un testo blu I
contenuti generati dall'intelligenza artificiale potrebbero non essere
corretti.](./media/image3.png)

4.  Nella finestra **Stay signed in?**, fare clic sul pulsante **Yes**.

![](./media/image4.png)

5.  Verrà visualizzata la home page di Power BI.

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image5.png)

**Attività 2: Avviare la versione di valutazione di Microsoft Fabric**

Seguire questi passaggi per iniziare la prova di Fabric.

1.  Nella pagina **Fabric**, fare clic su **Account manager** sul lato
    destro. Nel pannello Gestione account passare e selezionare **Start
    trial** come illustrato nell'immagine seguente.

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image6.png)

2.  Se richiesto, accettare i termini e quindi selezionare **Activate.**

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image7.png)

3.  Una volta che la capacità di prova è pronta, si riceve un messaggio
    di conferma. Selezionare **Got it** per iniziare a lavorare in
    Fabric.

> ![A white background with black text AI-generated content may be
> incorrect.](./media/image8.png)

4.  Aprire di nuovo il tuo Account Manager. Si noti che ora è presente
    un'intestazione per **Trial status**. Il tuo account manager tiene
    traccia del numero di giorni rimanenti nel periodo di prova. Vedrai
    anche il conto alla rovescia nella barra dei menu Fabric quando
    lavori in un'esperienza di prodotto.

> ![Uno screenshot di un computer I contenuti generati dall'intelligenza
> artificiale potrebbero non essere corretti.](./media/image9.png)

**Esercizio 2: Creare e implementare una lakehouse end-to-end per
l'organizzazione**

**Attività 1: Creare un'area di lavoro Fabric**

In questa attività viene creata un'area di lavoro Fabric. L'area di
lavoro contiene tutti gli elementi necessari per questa esercitazione
lakehouse, che include lakehouse, flussi di dati, pipeline di Data
Factory, notebook, set di dati di Power BI e report.

1.  Home page del Fabric, selezionare riquadro **+New workspace**.

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image5.png)

2.  Nel riquadro ** Create a workspace** visualizzato sul lato destro,
    immettere i dettagli seguenti e fare clic sul pulsante **Apply**.

[TABLE]

3.  Nota: per trovare l'ID istantaneo del tuo laboratorio, selezionare
    "Help" e copiare l'ID istantaneo.

> ![Uno screenshot di un computer I contenuti generati dall'intelligenza
> artificiale potrebbero non essere corretti.](./media/image10.png)
>
> ![Uno screenshot di un computer I contenuti generati dall'intelligenza
> artificiale potrebbero non essere corretti.](./media/image11.png)
>
> ![Uno screenshot di un computer I contenuti generati dall'intelligenza
> artificiale potrebbero non essere corretti.](./media/image12.png)
>
> ![Uno screenshot di un computer I contenuti generati dall'intelligenza
> artificiale potrebbero non essere corretti.](./media/image13.png)

4.  Attendere il completamento della distribuzione. Ci vogliono 2-3
    minuti per completarlo.

**Compito 2: Creare una lakehouse**

1.  Creare una nuova lakehouse facendo clic sul pulsante **+New item**
    nella barra di navigazione.

> ![Uno screenshot di un browser I contenuti generati dall'intelligenza
> artificiale potrebbero non essere corretti.](./media/image14.png)

2.  Fare clic sul riquadro "**Lakehouse**".

> ![Uno screenshot di un computer I contenuti generati dall'intelligenza
> artificiale potrebbero non essere corretti.](./media/image15.png)

3.  Nella finestra di dialogo **New lakehouse**, inserire
    **wwilakehouse** nel campo **Name**, fare clic sul pulsante
    **Create** e aprire la nuova lakehouse.

**NOTA**: Assicurati di rimuovere lo spazio prima di **wwilakehouse**.

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image16.png)

> ![Uno screenshot di un computer I contenuti generati dall'intelligenza
> artificiale potrebbero non essere corretti.](./media/image17.png)

4.  Verrà visualizzata una notifica che indica **Successfully created
    SQL endpoint**.

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image18.png)

**Attività 3: Inserire dati di esempio**

1.  Nella pagina **wwilakehouse**, andare alla sezione **Get data in
    your lakehouse** e fare clic su **Upload files as shown in the below
    image.**

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image19.png)

2.  Nella scheda **Upload files**, fare clic sulla cartella sotto
    **Files**

![Un oggetto rettangolare in bianco e nero con testo I contenuti
generati dall'intelligenza artificiale potrebbero non essere
corretti.](./media/image20.png)

3.  Passare a **C:\LabFiles** sulla VM, quindi selezionare
    ***dimension_customer.csv*** file e fare clic sul pulsante **Open**.

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image21.png)

4.  Quindi, fare clic sul pulsante **Upload **e chiudere

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image22.png)

5.  Fare clic e selezionare **Refresh** su **Files**. Viene visualizzato
    il file.

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image23.png)

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image24.png)

6.  Nella pagina **Lakehouse**, nel riquadro **Explorer**, selezionare
    **Files**. Ora, tuttavia, il mouse per **dimension_customer.csv**
    file. Fare clic sulle ellissi orizzontali **(...)** accanto a
    **dimension_customer**.csv. Navigare e fare clic su ** Load Table**,
    quindi selezionare **New table.**

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image25.png)

7.  Nella finestra di dialogo **Load file to new table**, fare clic sul
    pulsante **Load**.

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image26.png)

8.  Quindi, nel riquadro **Lakehouse Explorer** a sinistra, nella
    finestra **...** per il nodo **Table**, selezionare **Refresh**.

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image27.png)

![Uno screenshot di un motore di ricerca I contenuti generati
dall'intelligenza artificiale potrebbero non essere
corretti.](./media/image28.png)

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image29.png)

9.  È inoltre possibile utilizzare l'endpoint SQL del lakehouse per
    eseguire query sui dati con istruzioni SQL. Selezionare ** SQL
    analytics endpoint** dal menu a discesa **Lakehouse** nella parte
    superiore destra dello schermo.

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image30.png)

10. Nella pagina wwilakehouse, in **Explorer**, selezionare la tabella
    **dimension_customer** per visualizzare in anteprima i dati e
    selezionare **New SQL query** per scrivere le istruzioni SQL.

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image31.png)

11. Nella query di esempio seguente il conteggio delle righe viene
    aggregato in base alla ** BuyingGroup column** della tabella
    dimension_customer. I file di query SQL vengono salvati
    automaticamente per riferimento futuro ed è possibile rinominarli o
    eliminarli in base alle proprie esigenze. Incollare il codice come
    mostrato nell'immagine sottostante, quindi fare clic sull'icona di
    riproduzione per **eseguire** lo script.

> SELECT BuyingGroup, Count(\*) AS Total 
>
> FROM dimension_customer 
>
> GROUP BY BuyingGroup 

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image32.png)

**Nota**: se si verifica un errore durante l'esecuzione dello script,
eseguire un controllo incrociato della sintassi dello script con
l'immagine precedente.

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image32.png)

12. In precedenza, tutte le tabelle e le viste lakehouse venivano
    aggiunte automaticamente al modello semantico. Con i recenti
    aggiornamenti, per le nuove lakehouse, è necessario aggiungere
    manualmente le tabelle al modello semantico.

13. Nella scheda lakehouse **Reporting,** selezionare **Manage default
    Power BI semantic model** e selezionare le tabelle da aggiungere al
    modello semantico.

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image33.png)

14. Nella scheda **Manage default semantic model**, selezionare la
    tabella **dimension_customer** e fare clic su **Confirm.**

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image34.png)

**Attività 4: Creare un report**

1.  Ora, fare clic su **Fabric Lakehouse Tutorial-XX** nel riquadro di
    navigazione a sinistra.

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image35.png)

2.  Nella vista **Fabric Lakehouse Tutorial-XX**, selezionare
    **wwilakehouse** di Tipo **Semantic model(default).** Questo set di
    dati viene creato automaticamente e ha lo stesso nome della
    lakehouse.

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image36.png)

3.  Dal riquadro del modello semantico è possibile visualizzare tutte le
    tabelle. Sono disponibili opzioni per creare report da zero, report
    impaginati o consentire a Power BI di creare automaticamente un
    report basato sui dati. Per questa esercitazione, in **Explore this
    data,** selezionare ** Auto-create a report** come illustrato
    nell'immagine seguente.

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image37.png)

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image38.png)

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image39.png)

4.  Poiché la tabella è una dimensione e non contiene misure, Power BI
    crea una misura per il conteggio delle righe e la aggrega in colonne
    diverse, quindi crea grafici diversi, come illustrato nell'immagine
    seguente.

5.  Salvare questo report per il futuro selezionando **Save **dalla
    barra multifunzione superiore.

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image40.png)

6.  Nella finestra di dialogo **Save your replort **, inserire un nome
    per il tuo rapporto come +++dimension_customer-report+++ e
    selezionare **Save.**

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image41.png)

7.  Vedrai una notifica che indica **Report saved.**

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image42.png)

**Esercizio 3: Inserimento di dati nella lakehouse**

In questo esercizio, acquisirete ulteriori tabelle dimensionali e dei
fatti dai Wide World Importers (WWI) nella lakehouse.

**Attività 1: Inserire dati**

1.  Selezionare **Workspaces **nel riquadro di navigazione a sinistra,
    quindi selezionare la tua nuova area di lavoro (ad esempio, Fabric
    Lakehouse Tutorial-XX) dal menu **Workspaces**. Viene visualizzata
    la visualizzazione degli elementi dell'area di lavoro.

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image43.png)

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image44.png)

2.  Nella pagina **Fabric Lakehouse Tutorial-XX**, navigare e fare clic
    sul pulsante **+ New item**, quindi selezionare **Data pipeline.**

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image45.png)

3.  Nella finestra di dialogo **New pipeline**, specificare il nome come
    **+++IngestDataFromSourceToLakehouse+++ **e selezionare **Create.**
    Viene creata e aperta una nuova pipeline di data factory

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image46.png)

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image47.png)

4.  Nella pipeline di data factory appena creata, ad esempio
    **IngestDataFromSourceToLakehouse**, selezionare**  Copy data
    assistant**.

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image48.png)

5.  Quindi, configurare una connessione **HTTP** per importare i dati di
    esempio di World Wide Importers nella Lakehouse. Dall'elenco di
    **New sources**, selezionare **View more**, cercare **Http** e
    selezionarlo.

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image49.png)

6.  Nella finestra **Connect to data source,** immettere i dettagli
    della tabella seguente e selezionare **Next**.

[TABLE]

7.  ![Uno screenshot di un computer I contenuti generati
    dall'intelligenza artificiale potrebbero non essere
    corretti.](./media/image50.png)

8.  Nel passaggio successivo, abilitare la **Binary copy** e scegliere
    **ZipDeflate (.zip)** come **Compression type** poiché l'origine è
    un file .zip. Mantenere gli altri campi ai valori predefiniti e fare
    clic su **Next**.

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image51.png)

9.  Nella finestra ** Connect to data destination,** selezionare
    ** OneLake data hub** e selezionare **wwilakehouse.** Ora specifica
    **Root folder** come **Files** e fare clic su **Next**. Questo
    scriverà i dati nella sezione **Files** della lakehouse.

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image52.png)

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image53.png)

10. Scegliere **File format** che deve essere vuoto per la destinazione.
    Fare clic su **Next**, quindi su **Save+Run**. È possibile
    pianificare le pipeline per l'aggiornamento periodico dei dati. In
    questa esercitazione la pipeline viene eseguita una sola volta. Il
    processo di copia dei dati richiede circa 15-19 minuti per essere
    completato. ![A screenshot of a computer AI-generated content may be
    incorrect.](./media/image54.png) ![A screenshot of a computer
    AI-generated content may be incorrect.](./media/image55.png)

11. Ora, puoi vedere che la convalida fallirà. Fare clic su **pipeline
    validation output**

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image56.png)

12. Nella scheda **destination**, fare scorrere il menu a discesa **File
    format** e selezionare **Binary**.

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image57.png)

13. Ora fare clic sul pulsante **Run** ![Uno screenshot di un computer I
    contenuti generati dall'intelligenza artificiale potrebbero non
    essere corretti.](./media/image58.png) ![Uno screenshot di un
    computer I contenuti generati dall'intelligenza artificiale
    potrebbero non essere corretti.](./media/image59.png)

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image60.png)

14. Nella scheda Output, selezionare **Copy_a1n** per visualizzare i
    dettagli del trasferimento dei dati. Dopo aver visualizzato
    **Status** come **Succeeded**, fare clic sul pulsante **Close**.

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image61.png)

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image62.png)

15. Dopo l'esecuzione riuscita della pipeline, andare alla tua lakehouse
    (**wwilakehouse**) e aprire l'explorer per vedere i dati importati.

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image63.png)

16. Verificare che la cartella **WideWorldImportersDW** sia presente
    nella vista **Explorer **e contenga i dati per tutte le tabelle.

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image64.png)

17. I dati vengono creati nella sezione **Files** del lakehouse
    explorer. Una nuova cartella con GUID contiene tutti i dati
    necessari. Rinominare il GUID in +++wwi-raw-data+++

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image65.png)

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image66.png)

**Esercizio 4: Preparare e trasformare i dati nel lakehouse**

**Attività 1: Preparare i dati**

Dai passaggi precedenti dell'esercizio, abbiamo dati non elaborati
acquisiti dall'origine alla sezione **Files** della lakehouse. A questo
punto è possibile trasformare i dati e prepararli per la creazione di
tabelle differenziali.

1.  Ora, fare clic su **Fabric Lakehouse Tutorial-XX** nel riquadro di
    navigazione a sinistra.

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image67.png)

2.  2.  Nella **Home** page, andare alla sezione **Import**, clicca su
        **Notebook** e clicca su **From this computer**

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image68.png)

3.  Selezionare **Upload **dalla sezione **Import** navigare, fare clic
    su **Notebook** e fare clic su **From this computer**

**Nota**: Assicurati di selezionare **Tutti i file (\*.\*)** dal menu a
discesa accanto al campo **File name**

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image69.png)

4.  Navigare e selezionare **01-Create Delta Tables, 02-Data
    Transformation-Business Aggregation** notebook da **C:\LabFiles** e
    fare clic sul pulsante **Open**.

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image70.png)

5.  Verrà visualizzata una notifica che indica **Imported
    successfully.**

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image71.png)

6.  Dopo che l'importazione è riuscita, per visualizzare i blocchi
    appunti appena importati, selezionare **Fabric Lakehouse
    Tutorial-XX** nella sezione **Recommended**.

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image72.png)

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image73.png)

7.  Nel riquadro **Fabric Lakehouse Tutorial-XX**, selezionare
    **wwilakehouse** lakehouse per aprirlo.

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image74.png)

**Attività 2: Trasformare i dati e caricarli in tabella Delta argento**

1.  Nella pagina **wwilakehouse**, naviga e fare clic su **Open
    notebook** nella barra dei comandi, quindi selezionare **Existing
    notebook**.

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image75.png)

2.  Nell'elenco  **Open existing notebook,** selezionare il notebook
    **01 - Create Delta Tables** e selezionare **Open**.

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image76.png)

3.  Nel notebook aperto in **Lakehouse explorer** si noterà che il
    notebook è già collegato alla lakehouse aperta.

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image77.png)

\*\*Nota\*\*

Fabric offre la funzionalità
[**V-order**](https://learn.microsoft.com/en-us/fabric/data-engineering/delta-optimization-and-v-order)
per scrivere file delta lake ottimizzati. L'ordine V spesso migliora la
compressione di tre o quattro volte e fino a 10 volte l'accelerazione
delle prestazioni rispetto ai file Delta Lake che non sono ottimizzati.
Spark in Fabric ottimizza dinamicamente le partizioni durante la
generazione di file con una dimensione predefinita di 128 MB. Le
dimensioni del file di destinazione possono essere modificate in base ai
requisiti del carico di lavoro utilizzando le configurazioni. Con la
[**capacità di
ottimizzazione**](https://learn.microsoft.com/en-us/fabric/data-engineering/delta-optimization-and-v-order#what-is-optimized-write)
della scrittura, il motore Apache Spark riduce il numero di file scritti
e mira ad aumentare le dimensioni dei singoli file dei dati scritti.

4.  Prima di scrivere i dati come tabelle delta lake nella sezione
    **Tables **del lakehouse, si utilizzano due funzionalità Fabric
    (**V-order** e **Optimize Write**) per ottimizzare la scrittura dei
    dati e migliorare le prestazioni di lettura. Per abilitare queste
    funzionalità nella sessione, impostare queste configurazioni nella
    prima cella del notebook.

5.  Per avviare il notebook ed eseguire la cella, selezionare l'icona
    **Run **che viene visualizzata a sinistra della cella al passaggio
    del mouse.

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image78.png)

Durante l'esecuzione di una cella, non è necessario specificare i
dettagli del pool o del cluster Spark sottostante perché Fabric li
fornisce tramite Live Pool. Ogni area di lavoro Fabric viene fornita con
un pool Spark predefinito, denominato Live Pool. Ciò significa che
quando si creano notebook, non è necessario preoccuparsi di specificare
le configurazioni di Spark o i dettagli del cluster. Quando si esegue il
primo comando del notebook, il pool live è attivo e funzionante in pochi
secondi. E la sessione Spark viene stabilita e inizia a eseguire il
codice. L'esecuzione successiva del codice è quasi istantanea in questo
notebook mentre la sessione Spark è attiva.![Uno screenshot di un
computer I contenuti generati dall'intelligenza artificiale potrebbero
non essere corretti.](./media/image79.png)

6.  Successivamente, si leggono i dati non elaborati dalla sezione
    **Files** del lakehouse e si aggiungono altre colonne per le diverse
    parti di data come parte della trasformazione. Si usa l'API
    partitionBy Spark per partizionare i dati prima di scriverli come
    tabella delta in base alle colonne della parte dati appena create
    (anno e trimestre).

7.  Per eseguire la seconda cella, selezionare l'icona **Run** che
    appare a sinistra della cella al passaggio del mouse.

**Nota**: nel caso in cui non si riesca a visualizzare l'output, fare
clic sulle linee orizzontali sul lato sinistro dei **Spark jobs**.

\`\`\` 

from pyspark.sql.functions import col, year, month, quarter 

 

table_name = 'fact_sale' 

 

df =
spark.read.format("parquet").load('Files/wwi-raw-data/full/fact_sale_1y_full') 

df = df.withColumn('Year', year(col("InvoiceDateKey"))) 

df = df.withColumn('Quarter', quarter(col("InvoiceDateKey"))) 

df = df.withColumn('Month', month(col("InvoiceDateKey"))) 

 

df.write.mode("overwrite").format("delta").partitionBy("Year","Quarter").save("Tables/" +
table_name) 

\`\`\` 

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image80.png) ![A
screenshot of a computer AI-generated content may be
incorrect.](./media/image81.png)

8.  Dopo il caricamento delle tabelle dei fatti, è possibile passare al
    caricamento dei dati per il resto delle dimensioni. La cella
    seguente crea una funzione per leggere i dati non elaborati dalla
    sezione **Files** della lakehouse per ognuno dei nomi di tabella
    passati come parametro. Successivamente, crea un elenco di tabelle
    delle dimensioni. Infine, scorre l'elenco di tabelle e crea una
    tabella delta per ogni nome di tabella letto dal parametro di input.

9.  Seleziona la cella, sostituisci il codice e fai clic sull'icona
    **Run** che appare a sinistra della cella quando ci passi sopra con
    il mouse

&nbsp;

10. \`\`\` 

&nbsp;

11. from pyspark.sql.types import \* 

&nbsp;

12. def loadFullDataFromSource(table_name): 

&nbsp;

13.     df =
    spark.read.format("parquet").load('Files/wwi-raw-data/full/' +
    table_name) 

&nbsp;

14.     df.write.mode("overwrite").format("delta").save("Tables/" +
    table_name) 

&nbsp;

15.  

&nbsp;

16. full_tables = \[ 

&nbsp;

17.     'dimension_city', 

&nbsp;

18.     'dimension_date', 

&nbsp;

19.     'dimension_employee', 

&nbsp;

20.     'dimension_stock_item' 

&nbsp;

21.     \] 

&nbsp;

22.  

&nbsp;

23. for table in full_tables: 

&nbsp;

24.     loadFullDataFromSource(table) 

&nbsp;

25. \`\`\` 

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image82.png) ![A
screenshot of a computer AI-generated content may be
incorrect.](./media/image83.png)

26. Per convalidare le tabelle create, fare clic su e selezionare
    **refresh** nella finestra **Tables**. Vengono visualizzate le
    tabelle. 

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image84.png) ![Uno
screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image85.png)

27. Andare di nuovo alla vista degli elementi dell'area di lavoro,
    selezionare **Fabric Lakehouse Tutorial-XX** e selezionare lakehouse
    **wwilakehouse** per aprirla.

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image86.png)

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image87.png)

28. Ora aprire il secondo notebook. Nella visualizzazione lakehouse
    visualizzare a discesa il **Open notebook** e selezionare **Existing
    notebook** dal menu di spostamento superiore.

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image88.png)

29. Dall'elenco di **Open existing notebook**, selezionare il notebook
    ** 02 - Data Transformation - Business Aggregation** e fare clic sul
    pulsante **Open**.

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image89.png)

30. Nel notebook aperto in **Lakehouse explorer** si noterà che il
    notebook è già collegato alla lakehouse aperta.

31. Per avviare il notebook e selezionare la cella 1^(st) e selezionare
    l' icona **Run **che appare a sinistra della cella al passaggio del
    mouse.

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image90.png)

32. Un'organizzazione potrebbe avere ingegneri dei dati che lavorano con
    Scala/Python e altri ingegneri dei dati che lavorano con SQL (Spark
    SQL o T-SQL), tutti che lavorano sulla stessa copia dei dati. Fabric
    permette a questi diversi gruppi, con esperienze e preferenze
    diverse, di lavorare e collaborare. I due diversi approcci
    trasformano e generano aggregati aziendali. Puoi scegliere quello
    adatto a te o combinare questi approcci in base alle tue preferenze
    senza compromettere le prestazioni:

    1.  **Approccio \#1** - Utilizzare PySpark per unire e aggregare i
        dati per generare aggregazioni aziendali. Questo approccio è
        preferibile a qualcuno con un background di programmazione
        (Python o PySpark).

    2.  **Approccio \#2** - Utilizzare Spark SQL per unire e aggregare i
        dati per generare aggregazioni aziendali. Questo approccio è
        preferibile a un utente con background SQL, che passa a Spark.

33. **Approccio \#1 (sale_by_date_city)** - Utilizzare PySpark per unire
    e aggregare i dati per generare aggregazioni aziendali. Con il
    codice seguente si creano tre diversi frame di dati Spark, ognuno
    dei quali fa riferimento a una tabella delta esistente. Quindi si
    uniscono queste tabelle utilizzando i frame di dati, si esegue il
    raggruppamento per generare l'aggregazione, si rinominano alcune
    colonne e infine si scrive come tabella delta nella sezione
    **Tables **del lakehouse per persistere con i dati.

In questa cella vengono creati tre diversi frame di dati Spark, ognuno
dei quali fa riferimento a una tabella delta esistente.

df_fact_sale = spark.read.table("wwilakehouse.fact_sale")  

df_dimension_date = spark.read.table("wwilakehouse.dimension_date") 

df_dimension_city = spark.read.table("wwilakehouse.dimension_city") 

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image91.png)

34. In questa cella, si uniscono queste tabelle utilizzando i frame di
    dati creati in precedenza, si esegue il raggruppamento per generare
    l'aggregazione, si rinominano alcune colonne e infine si scrive come
    tabella delta nella sezione **Tables **della lakehouse.

&nbsp;

35. \`\`\` 

&nbsp;

36. sale_by_date_city = df_fact_sale.alias("sale") \\

&nbsp;

37. .join(df_dimension_date.alias("date"), df_fact_sale.InvoiceDateKey
    == df_dimension_date.Date, "inner") \\

&nbsp;

38. .join(df_dimension_city.alias("city"), df_fact_sale.CityKey ==
    df_dimension_city.CityKey, "inner") \\

&nbsp;

39. .select("date.Date", "date.CalendarMonthLabel", "date.Day",
    "date.ShortMonth", "date.CalendarYear", "city.City",
    "city.StateProvince",  

&nbsp;

40.  "city.SalesTerritory", "sale.TotalExcludingTax", "sale.TaxAmount",
    "sale.TotalIncludingTax", "sale.Profit")\\

&nbsp;

41. .groupBy("date.Date", "date.CalendarMonthLabel", "date.Day",
    "date.ShortMonth", "date.CalendarYear", "city.City",
    "city.StateProvince",  

&nbsp;

42.  "city.SalesTerritory")\\

&nbsp;

43. .sum("sale.TotalExcludingTax", "sale.TaxAmount",
    "sale.TotalIncludingTax", "sale.Profit")\\

&nbsp;

44. .withColumnRenamed("sum(TotalExcludingTax)",
    "SumOfTotalExcludingTax")\\

&nbsp;

45. .withColumnRenamed("sum(TaxAmount)", "SumOfTaxAmount")\\

&nbsp;

46. .withColumnRenamed("sum(TotalIncludingTax)",
    "SumOfTotalIncludingTax")\\

&nbsp;

47. .withColumnRenamed("sum(Profit)", "SumOfProfit")\\

&nbsp;

48. .orderBy("date.Date", "city.StateProvince", "city.City") 

&nbsp;

49.  

&nbsp;

50. sale_by_date_city.write.mode("overwrite").format("delta").option("overwriteSchema",
    "true").save("Tables/aggregate_sale_by_date_city") 

&nbsp;

51. \`\`\` 

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image92.png)

52. **Approccio \#2 (sale_by_date_employee)** - Utilizzare Spark SQL per
    unire e aggregare i dati per generare aggregazioni aziendali. Con il
    codice seguente si crea una visualizzazione Spark temporanea unendo
    tre tabelle, si esegue il raggruppamento per generare l'aggregazione
    e si rinominano alcune colonne. Infine, si legge dalla
    visualizzazione Spark temporanea e infine la si scrive come tabella
    delta nella sezione **Tables **della lakehouse per persistere con i
    dati.

In questa cella si crea una visualizzazione Spark temporanea unendo tre
tabelle, si esegue il raggruppamento per generare l'aggregazione e si
rinominano alcune colonne.

\`\`\` 

%%sql 

CREATE OR REPLACE TEMPORARY VIEW sale_by_date_employee 

AS 

SELECT 

       DD.Date, DD.CalendarMonthLabel 

 , DD.Day, DD.ShortMonth Month, CalendarYear Year 

      ,DE.PreferredName, DE.Employee 

      ,SUM(FS.TotalExcludingTax) SumOfTotalExcludingTax 

      ,SUM(FS.TaxAmount) SumOfTaxAmount 

      ,SUM(FS.TotalIncludingTax) SumOfTotalIncludingTax 

      ,SUM(Profit) SumOfProfit  

FROM wwilakehouse.fact_sale FS 

INNER JOIN wwilakehouse.dimension_date DD ON FS.InvoiceDateKey =
DD.Date 

INNER JOIN wwilakehouse.dimension_Employee DE ON FS.SalespersonKey =
DE.EmployeeKey 

GROUP BY DD.Date, DD.CalendarMonthLabel, DD.Day, DD.ShortMonth,
DD.CalendarYear, DE.PreferredName, DE.Employee 

ORDER BY DD.Date ASC, DE.PreferredName ASC, DE.Employee ASC 

\`\`\` 

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image93.png)

53. In questa cella si legge dalla visualizzazione Spark temporanea
    creata nella cella precedente e infine la si scrive come tabella
    delta nella sezione **Tables **della lakehouse.

&nbsp;

54. sale_by_date_employee = spark.sql("SELECT \* FROM
    sale_by_date_employee") 

&nbsp;

55. sale_by_date_employee.write.mode("overwrite").format("delta").option("overwriteSchema",
    "true").save("Tables/aggregate_sale_by_date_employee") 

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image94.png)

56. Per convalidare le tabelle create, fare clic su e selezionare
    **Refresh** nella finestra **Tables**. Vengono visualizzate le
    tabelle aggregate.

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image95.png)

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image96.png)

Entrambi gli approcci producono un risultato simile. Puoi scegliere in
base al tuo background e alle tue preferenze, per ridurre al minimo la
necessità di apprendere una nuova tecnologia o scendere a compromessi
sulle prestazioni.

Inoltre, potresti notare che stai scrivendo dati come file delta lake.
La funzione di individuazione e registrazione automatica delle tabelle
di Fabric le preleva e le registra nel metastore. Non è necessario
chiamare in modo esplicito le istruzioni CREATE TABLE per creare tabelle
da utilizzare con SQL.

**Esercizio 5: Creazione di report in Microsoft Fabric**

In questa sezione dell'esercitazione viene creato un modello di dati di
Power BI e viene creato un report da zero.

**Attività 1: Esplorare i dati nel livello argento utilizzando
l'endpoint SQL**

Power BI è integrato in modo nativo nell'intera esperienza Fabric.
Questa integrazione nativa offre una modalità univoca, denominata
DirectLake, per l'accesso ai dati dal lakehouse per offrire l'esperienza
di query e creazione di report più efficiente. La modalità DirectLake è
una nuova funzionalità del motore rivoluzionaria per analizzare set di
dati di grandi dimensioni in Power BI. La tecnologia si basa sull'idea
di caricare file in formato parquet direttamente da un data lake senza
dover interrogare un data warehouse o un endpoint lakehouse e senza
dover importare o duplicare i dati in un set di dati di Power BI.
DirectLake è un percorso rapido per caricare i dati dal data lake
direttamente nel motore di Power BI, pronti per l'analisi.

Nella modalità DirectQuery tradizionale, il motore di Power BI esegue
direttamente una query sui dati dall'origine per eseguire ogni query e
le prestazioni della query dipendono dalla velocità di recupero dei
dati. DirectQuery elimina la necessità di copiare i dati, garantendo che
eventuali modifiche nell'origine si riflettano immediatamente nei
risultati della query durante l'importazione. D'altra parte, in modalità
di importazione le prestazioni sono migliori perché i dati sono
prontamente disponibili in memoria senza eseguire query sui dati
dall'origine per ogni esecuzione di query. Tuttavia, il motore di Power
BI deve prima copiare i dati in memoria durante l'aggiornamento dei
dati. Durante l'aggiornamento successivo dei dati (sia
nell'aggiornamento pianificato che in quello su richiesta) vengono
rilevate solo le modifiche apportate all'origine dati sottostante.

La modalità DirectLake elimina ora questo requisito di importazione
caricando i file di dati direttamente in memoria. Poiché non esiste un
processo di importazione esplicito, è possibile rilevare eventuali
modifiche all'origine non appena si verificano, combinando così i
vantaggi di DirectQuery e della modalità di importazione evitando gli
svantaggi. La modalità DirectLake è quindi la scelta ideale per
l'analisi di set di dati molto grandi e set di dati con aggiornamenti
frequenti all'origine.

1.  Nel riquadro di spostamento sinistro selezionare
    **Fabric_LakehouseXX** e quindi selezionare **wwilakehouse** di
    **Type SQL analytics endpoint.**

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image97.png)

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image98.png)

2.  Dal riquadro dell'endpoint SQL dovrebbe essere possibile
    visualizzare tutte le tabelle create. Se non li vedi ancora,
    selezionare l'icona **Refresh **in alto. Selezionare quindi la
    scheda** Model layout** nella parte inferiore per aprire il set di
    dati di Power BI predefinito.

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image99.png)

3.  Per questo modello di dati, è necessario definire la relazione tra
    tabelle diverse in modo da poter creare report e visualizzazioni
    basati sui dati provenienti da tabelle diverse. Fare clic su **Auto
    layout**

> ![Uno screenshot di un computer I contenuti generati dall'intelligenza
> artificiale potrebbero non essere
> corretti.](./media/image100.png) ![Uno screenshot di un computer I
> contenuti generati dall'intelligenza artificiale potrebbero non essere
> corretti.](./media/image101.png)

4.  Dalla tabella **fact_sale**, trascinare il campo **CityKey** e
    rilasciarlo sul campo **CityKey** nella tabella **dimension_city**
    per creare una relazione. Viene visualizzata la finestra di dialogo
    **Create Relationship**.

Nota: Riordinare le tabelle facendo clic sulla tabella, trascinandola e
rilasciandola per avere le tabelle dimension_city e fact_sale una
accanto all'altra. Lo stesso vale per qualsiasi due tabelle che stai
cercando di creare una relazione. Questo serve solo per rendere più
facile il trascinamento della selezione delle colonne tra le
tabelle.![Uno screenshot di un computer I contenuti generati
dall'intelligenza artificiale potrebbero non essere
corretti.](./media/image102.png)

5.  Nella finestra di dialogo **Create Relationship**:

    - **Table 1** è popolata con **fact_sale** e la colonna di
      **CityKey**.

    - **Table 2** è popolata con **dimension_city** e la colonna di
      **CityKey**.

    - Cardinality: **Many to one (\*:1)** 

    - Cross filter direction: **Single** 

    - Lasciare selezionata la casella accanto a **Make this relationship
      active**.

    - Selezionare la casella accanto a **Assume referential integrity.**

    - Selezionare **Save.**

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image103.png)

6.  Aggiungere quindi queste relazioni con le stesse impostazioni
    **Create Relationship** illustrate in precedenza, ma con le tabelle
    e le colonne seguenti:

    - **StockItemKey(fact_sale)** -
      **StockItemKey(dimension_stock_item)**

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image104.png)

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image105.png)

- **Salespersonkey(fact_sale) - EmployeeKey(dimension_employee)**

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image106.png)

7.  Assicurati di creare le relazioni tra i due set seguenti utilizzando
    gli stessi passaggi di cui sopra.

- **CustomerKey(fact_sale)** - **CustomerKey(dimension_customer)** 

&nbsp;

- **InvoiceDateKey(fact_sale)** - **Date(dimension_date)** 

8.  Dopo aver aggiunto queste relazioni, il modello di dati dovrebbe
    essere come mostrato nell'immagine seguente ed è pronto per la
    creazione di report.

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image107.png)

**Attività 2: Crea Report**

1.  Sulla barra multifunzione superiore, selezionare **Reporting **e
    selezionare ** New report** per iniziare a creare report/dashboard
    in Power BI.

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image108.png)

![Screenshot di una casella dati I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image109.png)

2.  Nell'area di disegno del report di Power BI è possibile creare
    report per soddisfare i requisiti aziendali trascinando le colonne
    necessarie dal riquadro **Data **all'area di disegno e usando una o
    più visualizzazioni disponibili.

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image110.png)

**Aggiungere un titolo:**

3.  Nella barra multifunzione selezionare **Text box**. Digitare **WW
    Importers Profit Reporting**. **Evidenziare** il **testo** e
    aumentare le dimensioni a **20**.

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image111.png)

4.  Ridimensionare la casella di testo e posizionarla in **alto a
    sinistra** nella pagina del report, quindi fare clic all'esterno
    della casella di testo.

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image112.png)

**Aggiungere una Scheda:**

- Nel riquadro **Data**, espandere **fact_sales** e selezionare la
  casella accanto a **Profit**. Questa selezione crea un istogramma e
  aggiunge il campo all'asse Y.

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image113.png)

5.  Con il grafico a barre selezionato, selezionare l'oggetto visivo
    **Card** nel riquadro di visualizzazione.

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image114.png)

6.  Questa selezione converte l'oggetto visivo in una scheda. Posiziona
    la carta sotto il titolo.

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image115.png)

7.  Fare clic in un punto qualsiasi dell'area di disegno vuota (o premi
    il tasto Esc) in modo che la scheda che abbiamo appena posizionato
    non sia più selezionata.

**Aggiungere un Bar chart:**

8.  Nel riquadro **Data**, espandere **fact_sales** e selezionare la
    casella accanto a **Profit**. Questa selezione crea un istogramma e
    aggiunge il campo all'asse Y.

>  ![Screenshot di una casella di ricerca I contenuti generati
> dall'intelligenza artificiale potrebbero non essere
> corretti.](./media/image116.png)

9.  Nel riquadro **Data,** espandere **dimension_city** e selezionare la
    casella **SalesTerritory**. Questa selezione aggiunge il campo
    all'asse Y.

> ![Uno screenshot di un computer I contenuti generati dall'intelligenza
> artificiale potrebbero non essere corretti.](./media/image117.png)

10. Con il Bar chart selezionato, selezionare **Clustered bar chart**
    nel riquadro di visualizzazione. Questa selezione converte
    l'istogramma in un Bar chart.

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image118.png)

11. Ridimensionare il Bar chart per riempire l'area sotto il titolo e la
    scheda.

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image119.png)

12. Fare clic in un punto qualsiasi dell'area di disegno vuota (o
    premere il tasto Esc) in modo che il Bar chart non sia più
    selezionato.

**Creare un stacked area chart visual:**

13. Nel riquadro **Visualizations,** selezionare l'oggetto visivo
    **Stacked area chart**.

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image120.png)

14. Riposizionare e ridimensionare il grafico ad area in pila a destra
    degli oggetti visivi del grafico a schede e a barre creati nei
    passaggi precedenti.

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image121.png)

15. Nel riquadro **Data**, espandere **fact_sales** e selezionare la
    casella accanto a **Profit**. Espandere **dimension_date** e
    selezionare la casella accanto a **FiscalMonthNumber**. Questa
    selezione crea un grafico a linee piene che mostra il profitto per
    mese fiscale.

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image122.png)

16. Nel riquadro **Data **espandere **dimension_stock_item** e
    trascinare **BuyingPackage** nell'area del campo Legenda. Questa
    selezione aggiunge una riga per ciascuno dei pacchetti di acquisto.

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image123.png) ![Uno
screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image124.png)

17. Fare clic in un punto qualsiasi dell'area di disegno vuota (o
    premere il tasto ESC) in modo che il grafico ad area in pila non sia
    più selezionato.

**Crea un Column chart:**

18. Nel riquadro **Visualizations,** selezionare il visivo **Stacked
    column chart**.

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image125.png)

19. Nel riquadro **Data**, espandere **fact_sales** e selezionare la
    casella accanto a **Profit**. Questa selezione aggiunge il campo
    all'asse Y.

 

20. Nel riquadro **Data**, espandere **dimension_employee** e
    selezionare la casella accanto a **Employee**. Questa selezione
    aggiunge il campo all'asse X.

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image126.png)

21. Fare clic in un punto qualsiasi dell'area di disegno vuota (o
    premere il tasto Esc) in modo che il grafico non sia più
    selezionato.

22. Dalla barra multifunzione, selezionare **File** \> **Save**.

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image127.png)

23. Inserire il nome del report come **Profit Reporting**. Selezionare
    **Save**.

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image128.png)

24. Riceverai una notifica che indica che il rapporto è stato
    salvato.![Uno screenshot di un computer I contenuti generati
    dall'intelligenza artificiale potrebbero non essere
    corretti.](./media/image129.png)

**Esercizio 6: Pulire le risorse**

È possibile eliminare singoli report, pipeline, magazzini e altri
elementi oppure rimuovere l'intero workspace. Usare la procedura
seguente per eliminare l'area di lavoro creata per questa esercitazione.

1.  Selezionare il tuo spazio di lavoro, il **Fabric Lakehouse
    Tutorial-XX** dal menu di navigazione a sinistra. Apre la
    visualizzazione degli elementi dell'area di lavoro.

![Uno screenshot dello schermo di un computer I contenuti generati
dall'intelligenza artificiale potrebbero non essere
corretti.](./media/image130.png)

2.  Selezionare l'icona **...** sotto il nome dell'area di lavoro e
    selezionare **Workspace settings**.

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image131.png)

3.  Selezionare **Other **e **Remove this workspace.**

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image132.png)

4.  Fare clic su **Delete **nell'avviso che si apre.

![Uno sfondo bianco con testo nero I contenuti generati
dall'intelligenza artificiale potrebbero non essere
corretti.](./media/image133.png)

5.  Attendere una notifica che indica che l'area di lavoro è stata
    eliminata prima di passare al lab successivo.

![Uno screenshot di un computer I contenuti generati dall'intelligenza
artificiale potrebbero non essere corretti.](./media/image134.png)

**Riepilogo**: Questo laboratorio pratico si concentra sull'impostazione
e la configurazione di componenti essenziali all'interno di Microsoft
Fabric e Power BI per la gestione dei dati e la creazione di report.
Include attività come l'attivazione di versioni di valutazione, la
configurazione di OneDrive, la creazione di aree di lavoro e la
configurazione di lakehouse. Il lab illustra anche le attività correlate
all'inserimento di dati di esempio, all'ottimizzazione delle tabelle
delta e alla creazione di report in Power BI per un'analisi efficace dei
dati. Gli obiettivi mirano a fornire un'esperienza pratica nell'uso di
Microsoft Fabric e Power BI per la gestione dei dati e la creazione di
report.
