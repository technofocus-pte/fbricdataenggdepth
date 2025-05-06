**Introduction**

Apache Spark est un moteur open source pour le traitement des données
distribuées, et est largement utilisé pour explorer, traiter et analyser
d'énormes volumes de données dans le stockage de lacs de données. Spark
est disponible en tant qu'option de traitement dans de nombreux produits
de plateforme de données, notamment Azure HDInsight, Azure Databricks,
Azure Synapse Analytics et Microsoft Fabric. L'un des avantages de Spark
est la prise en charge d'un large éventail de langages de programmation,
notamment Java, Scala, Python et SQL ; ce qui fait de Spark une solution
très flexible pour les charges de travail de traitement des données,
notamment le nettoyage et la manipulation des données, l'analyse
statistique et l'apprentissage automatique, ainsi que l'analyse et la
visualisation des données.

Les tables d'un lakehouse Microsoft Fabric sont basées sur le format
open source *Delta Lake* pour Apache Spark. Delta Lake ajoute la prise
en charge de la sémantique relationnelle pour les opérations de données
par lots et en continu, et permet la création d'une architecture
Lakehouse dans laquelle Apache Spark peut être utilisé pour traiter et
interroger des données dans des tables basées sur des fichiers
sous-jacents dans un lac de données.

Dans Microsoft Fabric, les flux de données (Gen2) se connectent à
diverses sources de données et effectuent des transformations dans Power
Query Online. Ils peuvent ensuite être utilisés dans les pipelines de
données pour ingérer des données dans un lakehouse ou un autre magasin
analytique, ou pour définir un jeu de données pour un rapport Power BI.

Cet atelier est conçu pour présenter les différents éléments de
Dataflows (Gen2), et non pour créer une solution complexe qui peut
exister dans une entreprise.

**Objectives**:

- Créez un workspace dans Microsoft Fabric avec la version d'évaluation
  de Fabric activée.

- Établissez un environnement de maison de lac et téléchargez des
  fichiers de données pour analyse.

- Générez un Notebook pour l'exploration et l'analyse interactives des
  données.

- Chargez des données dans une trame de données pour un traitement et
  une visualisation ultérieurs.

- Appliquez des transformations aux données à l'aide de PySpark.

- Enregistrez et partitionnez les données transformées pour une
  interrogation optimisée.

- Créer une table dans le metastore Spark pour la gestion des données
  structurées

- Enregistrez DataFrame en tant que table delta gérée nommée «
  salesorders ».

- Enregistrez DataFrame en tant que table delta externe nommée «
  external_salesorder » avec un chemin d'accès spécifié.

- Décrivez et comparez les propriétés des tables managées et externes.

- Exécutez (Run) des requêtes SQL sur des tables à des fins d'analyse et
  de création de rapports.

- Visualisez les données à l'aide de bibliothèques Python telles que
  matplotlib et seaborn.

- Établissez un data lakehouse dans l'expérience Data Engineering et
  ingérez des données pertinentes pour une analyse ultérieure.

- Définissez un flux de données pour l'extraction, la transformation et
  le chargement des données dans le lakehouse.

- Configurez les destinations de données dans Power Query pour stocker
  les données transformées dans le lakehouse.

- Incorporez le flux de données dans un pipeline pour permettre le
  traitement et l'ingestion planifiés des données.

- Supprimez l'espace de travail (workspace) et les éléments associés
  pour conclure l'exercice.

# Exercice 1 : Créer un workspace, un lakehouse, un notebook et charger des données dans une trame de données 

## Tâche 1 : Créer un workspace 

Avant d'utiliser des données dans Fabric, créez un workspace avec la
version d'essai de Fabric activée.

1.  Ouvrez votre navigateur, accédez à la barre d'adresse et tapez ou
    collez l'URL suivante : <https://app.fabric.microsoft.com/> puis
    appuyez sur le bouton **Enter**.

> **Remarque** : Si vous êtes redirigé vers la page d'accueil de
> Microsoft Fabric, ignorez les étapes de \#2 à \#4.
>
> S![](./media/image1.png)

2.  Dans la fenêtre **Microsoft Fabric**, entrez vos informations
    d'identification, puis cliquez sur le bouton **Submit**.

> ![](./media/image2.png)

3.  Ensuite, dans la fenêtre **Microsoft**, entrez le mot de passe et
    cliquez sur le bouton **Sign in.**

> ![Un écran de connexion avec une boîte rouge et un texte bleu
> Description générée automatiquement](./media/image3.png)

4.  Dans **Stay signed in?**, cliquez sur le bouton **Yes**.

> ![Une capture d'écran d'une erreur informatique Description générée
> automatiquement](./media/image4.png)

5.  Page d'accueil du Fabric, sélectionnez vignette **+New workspace**.

> ![Une capture d'écran d'un ordinateur Le contenu généré par l'IA peut
> être incorrect.](./media/image5.png)

6.  Dans l'onglet **Create a workspace tab**, entrez les détails
    suivants et cliquez sur le bouton **Apply**.

[TABLE]

> ![Une capture d'écran d'un ordinateur Description générée
> automatiquement](./media/image6.png)
>
> ![](./media/image7.png)

![](./media/image8.png)

![](./media/image9.png)

7.  Attendez la fin du déploiement. Cela prend 2-3 minutes à compléter.
    Lorsque votre nouvel espace de travail (workspace) s'ouvre, il doit
    être vide.

## Tâche 2 : Créer une Lakehouse et télécharger des fichiers

Maintenant que vous disposez d'un workspace, il est temps de passer à
l'*expérience d'ingénierie des données* dans le portail et de créer un
data lakehouse pour les fichiers de données que vous allez analyser.

1.  Créez une nouvelle Eventhouse en cliquant sur le bouton **+New
    item** dans la barre de navigation.

![Une capture d'écran d'un navigateur Le contenu généré par l'IA peut
être incorrect.](./media/image10.png)

2.  Cliquez sur la tuile **« Lakehouse** ».

![Une capture d'écran d'un ordinateur Le contenu généré par l'IA peut
être incorrect.](./media/image11.png)

3.  Dans la boîte de dialogue **New lakehouse**, entrez
    **+++Fabric_lakehouse+++** dans le champ **Name**, cliquez sur le
    bouton **Create** et ouvrez le New lakehouse.

![Une capture d'écran d'un ordinateur Le contenu généré par l'IA peut
être incorrect.](./media/image12.png)

4.  Après environ une minute, une nouvelle maison de lac vide sera
    créée. Vous devez ingérer des données dans le data lakehouse à des
    fins d'analyse.

![](./media/image13.png)

5.  Une notification indiquant **Successfully created SQL endpoint**,
    s'affiche.

![](./media/image14.png)

6.  Dans la section **Explorer**, sous le **fabric_lakehouse**, passez
    votre souris à côté du **Files folder**, puis cliquez sur les points
    de suspension horizontaux **(...)** menu. Naviguez et cliquez sur
    **Upload**, puis cliquez sur le **Upload folder** comme indiqué dans
    l'image ci-dessous.

![](./media/image15.png)

7.  Dans le volet **Upload folder** qui s'affiche sur le côté droit,
    sélectionnez **folder icon** sous **Files/**, puis accédez à
    **C :\LabFiles**, puis sélectionnez le dossier **orders** et cliquez
    sur le bouton **Upload**.

![](./media/image16.png)

8.  Dans le cas, **Upload 3 files to this site?** s'affiche, puis
    cliquez sur le bouton **Upload**.

![](./media/image17.png)

9.  Dans le volet Upload le dossier, cliquez sur le bouton **Upload**.

> ![](./media/image18.png)

10. Une fois les fichiers chargés, **close** le volet **Upload folder**.

> ![](./media/image19.png)

11. Développez **Files**, sélectionnez le dossier **orders** et vérifiez
    que les fichiers CSV ont été téléchargés.

![](./media/image20.png)

## Tâche 3 : Créer un Notebook

Pour utiliser des données dans Apache Spark, vous pouvez créer un
*Notebook*. Les blocs-notes fournissent un environnement interactif dans
lequel vous pouvez écrire et exécuter du code (dans plusieurs langues)
et ajouter des notes pour le documenter.

1.  Sur la page **d'accueil (Home)**, lors de l'affichage du contenu du
    dossier **orders** dans votre datalake, dans le menu **Open
    notebook**, sélectionnez **New notebook**.

![](./media/image21.png)

2.  Après quelques secondes, un nouveau Notebook contenant une seule
    *cellule* s'ouvre. Les notebooks sont constitués d'une ou plusieurs
    cellules qui peuvent contenir du *code* ou du *markdown* (texte
    formaté).

![](./media/image22.png)

3.  Sélectionnez la première cellule (qui est actuellement une *cellule
    de code*), puis dans la barre d'outils dynamique en haut à droite,
    utilisez le bouton **M↓** pour **convert the cell to
    a markdown cell**.

![](./media/image23.png)

4.  Lorsque la cellule se transforme en cellule Markdown, le texte
    qu'elle contient est affiché.

![](./media/image24.png)

5.  Utilisez le **🖉** bouton (Modifier) pour passer la cellule en mode
    édition, remplacez tout le texte puis modifiez la démarque comme
    suit :

> CodeCopy
>
> \# Exploration des données de commande client
>
> Utilisez le code de ce notebook pour explorer les données de commande
> client.

![](./media/image25.png)

![Une capture d'écran d'un ordinateur Description générée
automatiquement](./media/image26.png)

6.  Cliquez n'importe où dans le Notebook en dehors de la cellule pour
    arrêter de le modifier et voir le markdown rendu.

![Une capture d'écran d'un ordinateur Description générée
automatiquement](./media/image27.png)

## Tâche 4 : Charger des données dans une trame de données

Vous êtes maintenant prêt à exécuter du code qui charge les données dans
une *trame de données*. Les dataframes dans Spark sont similaires aux
dataframes Pandas dans Python et fournissent une structure commune pour
travailler avec les données dans les lignes et les colonnes.

**Remarque** : Spark prend en charge plusieurs langages de codage,
notamment Scala, Java et autres. Dans cet exercice, nous allons utiliser
*PySpark*, qui est une variante de Python optimisée pour Spark. PySpark
est l'un des langages les plus couramment utilisés sur Spark et est le
langage par défaut dans les notebooks Fabric.

1.  Une fois le Notebook visible, développez la liste **Files** et
    sélectionnez le dossier **orders** afin que les fichiers CSV soient
    répertoriés à côté de l'éditeur de Notebook.

![](./media/image28.png)

2.  Maintenant, passez votre souris pour 2019.csv fichier. Cliquez sur
    les points de suspension horizontaux **(...)** à côté de 2019.csv.
    Naviguez et cliquez sur **Load data**, puis sélectionnez **Spark**.
    Une nouvelle cellule de code contenant le code suivant sera ajoutée
    au carnet :

> CodeCopy
>
> df =
> spark.read.format(« csv »).option(« en-tête »,"true »).load(« Fichiers/commandes/2019.csv »)
>
> \# df now est un DataFrame Spark contenant des données CSV de
> « Files/orders/2019.csv ».
>
> Affichage(DF)

![](./media/image29.png)

![](./media/image30.png)

**Astuce** : Vous pouvez masquer les volets de l'explorateur Lakehouse
sur la gauche en utilisant leurs **«** icônes ». Faire

Cela vous aidera à vous concentrer sur le carnet.

3.  Utilisez le bouton **▷ Run cell** à gauche de la cellule pour
    l'exécuter.

![](./media/image31.png)

**Remarque** : Comme c'est la première fois que vous exécutez (Run( du
code Spark, une session Spark doit être démarrée. Cela signifie que la
première exécution de la session peut prendre environ une minute. Les
exécutions suivantes seront plus rapides.

4.  Une fois la commande de cellule terminée, examinez le résultat sous
    la cellule, qui doit ressembler à ceci :

![](./media/image32.png)

5.  La sortie affiche les lignes et les colonnes de données du fichier
    2019.csv. Cependant, notez que les en-têtes de colonne ne sont pas
    corrects. Le code par défaut utilisé pour charger les données dans
    un dataframe suppose que le fichier CSV inclut les noms de colonne
    dans la première ligne, mais dans ce cas, le fichier CSV inclut
    uniquement les données sans informations d'en-tête.

6.  Modifiez le code pour définir l'option **header** sur **false**.
    Remplacez tout le code de la **cell** par le code suivant et cliquez
    sur le bouton **▷ Run cell** et vérifiez la sortie

> CodeCopy
>
> df =
> spark.read.format(« csv »).option(« en-tête »,"false »).load(« Fichiers/commandes/2019.csv »)
>
> \# df now est un DataFrame Spark contenant des données CSV de
> « Files/orders/2019.csv ».
>
> Affichage(DF)
>
> ![](./media/image33.png)

7.  Désormais, le dataframe inclut correctement la première ligne en
    tant que valeurs de données, mais les noms de colonne sont générés
    automatiquement et ne sont pas très utiles. Pour donner un sens aux
    données, vous devez définir explicitement le schéma et le type de
    données corrects pour les valeurs de données dans le fichier.

8.  Remplacez tout le code de la **cell** par le code suivant et cliquez
    sur le bouton **▷ Run cell** et vérifiez la sortie

> CodeCopy
>
> à partir de pyspark.sql.types import \*
>
> orderSchema = StructType(\[
>
> StructField(« SalesOrderNumber », StringType()),
>
> StructField(« SalesOrderLineNumber », IntegerType()),
>
> StructField(« OrderDate », DateType()),
>
> StructField(« CustomerName », StringType()),
>
> StructField(« email », StringType()),
>
> StructField(« Élément », StringType()),
>
> StructField(« Quantité », IntegerType()),
>
> StructField(« UnitPrice », FloatType()),
>
> StructField(« tax », floatType())
>
> \])
>
> df =
> spark.read.format(« csv »).schema(orderSchema).load(« Fichiers/commandes/2019.csv »)
>
> Affichage(DF)

![](./media/image34.png)

> ![](./media/image35.png)

9.  Désormais, la trame de données inclut les noms de colonne corrects
    (en plus de **Index**, qui est une colonne intégrée dans toutes les
    trames de données en fonction de la position ordinale de chaque
    ligne). Les types de données des colonnes sont spécifiés à l'aide
    d'un ensemble standard de types définis dans la bibliothèque SQL
    Spark, qui ont été importés au début de la cellule.

10. Vérifiez que vos modifications ont été appliquées aux données en
    affichant le dataframe.

11. Utilisez l'icône de **Code +** sous la sortie de la cellule pour
    ajouter une nouvelle cellule de code au Notebook et entrez-y le code
    suivant. Cliquez sur le bouton **▷ Run cell** et vérifiez la sortie
    (output)

> CodeCopy
>
> Affichage(DF)
>
> ![Une capture d'écran d'un ordinateur Description générée
> automatiquement](./media/image36.png)

12. Le dataframe inclut uniquement les données du fichier **2019.csv**.
    Modifiez le code afin que le chemin d'accès au fichier utilise un
    caractère générique \* pour lire les données de la commande client à
    partir de tous les fichiers du dossier **orders**

13. Utilisez l’icône **+ Code** sous la sortie de la cellule pour
    ajouter une nouvelle cellule de code au Notebook et entrez-y le code
    suivant.

CodeCopy

> à partir de pyspark.sql.types import \*
>
> orderSchema = StructType(\[
>
> StructField(« SalesOrderNumber », StringType()),
>
> StructField(« SalesOrderLineNumber », IntegerType()),
>
> StructField(« OrderDate », DateType()),
>
> StructField(« CustomerName », StringType()),
>
> StructField(« email », StringType()),
>
> StructField(« Élément », StringType()),
>
> StructField(« Quantité », IntegerType()),
>
> StructField(« UnitPrice », FloatType()),
>
> StructField(« tax », floatType())
>
> \])
>
> df =
> spark.read.format(« csv »).schema(orderSchema).load(« Fichiers/commandes/\*.csv »)
>
> Affichage(DF)

![](./media/image37.png)

14. Exécutez (Run) la cellule de code modifiée et examinez la sortie,
    qui doit maintenant inclure les ventes de 2019, 2020 et 2021.

![](./media/image38.png)

**Remarque** : Seul un sous-ensemble des lignes est affiché, il se peut
donc que vous ne puissiez pas voir les exemples de toutes les années.

# Exercice 2 : Explorer les données d'une trame de données

L'objet dataframe comprend un large éventail de fonctions que vous
pouvez utiliser pour filtrer, regrouper et manipuler les données qu'il
contient.

## Tâche 1 : Filtrer une trame de données

1.  Utilisez l'icône **de code +** sous la sortie de la cellule pour
    ajouter une nouvelle cellule de code au Notebook et entrez-y le code
    suivant.

**CodeCopy**

> customers = df\['NomduClient', 'E-mail'\]
>
> print(clients.count())
>
> print(clients.distinct().count())
>
> display(clients.distinct())
>
> ![](./media/image39.png)

2.  **Exécutez (Run)** la nouvelle cellule de code et examinez les
    résultats. Observez les détails suivants :

    - Lorsque vous effectuez une opération sur une trame de données, le
      résultat est une nouvelle datatrame (dans ce cas, une nouvelle
      **customers** dataframe clients est créée en sélectionnant un
      sous-ensemble spécifique de colonnes dans la datatrame **df**)

    - Les trames de données fournissent des fonctions telles que
      **count** et **distinct** qui peuvent être utilisées pour résumer
      et filtrer les données qu'elles contiennent.

    - La trame de données\['Field1', 'Field2', ...\] La syntaxe est une
      façon abrégée de définir un sous-ensemble de colonnes. Vous pouvez
      également utiliser la méthode **select**, de sorte que la première
      ligne du code ci-dessus puisse être écrite comme customers =
      df.select(« CustomerName », « Email »)

> ![](./media/image40.png)

3.  Modifiez le code, remplacez tout le code de la **cellule** par le
    code suivant et cliquez sur le bouton **▷ Run cell** comme suit :

> CodeCopy
>
> customers = df.select(« NomduClient »,
> « E-mail »).where(df\['Item'\]=='Route-250 Rouge, 52')
>
> print(clients.count())
>
> print(clients.distinct().count())
>
> display(clients.distinct())

4.  **Exécutez (Run)** le code modifié pour afficher les clients qui ont
    acheté ***Road-250 Red, 52* product**. Notez que vous pouvez «
    **chain** » plusieurs fonctions afin que la sortie d'une fonction
    devienne l'entrée de la suivante - dans ce cas, la trame de données
    créée par la méthode **select** est la trame de données source de la
    méthode **where** utilisée pour appliquer des critères de filtrage.

> ![](./media/image41.png)

## Tâche 2 : Agréger et regrouper des données dans une trame de données

1.  Cliquez sur **+ Code** et copiez et collez le code ci-dessous, puis
    cliquez sur le bouton **Run cell**.

CodeCopy

> productSales = df.select(« Article »,
> « Quantité »).groupBy(« Item »).sum()
>
> display(productSales)
>
> ![](./media/image42.png)

2.  Notez que les résultats indiquent la somme des quantités commandées
    regroupées par produit. La méthode **groupBy** regroupe les lignes
    par *élément*, et la fonction d'agrégation **sum** suivante est
    appliquée à toutes les colonnes numériques restantes (dans ce cas,
    *Quantity*)

![](./media/image43.png)

3.  Cliquez sur **+ Code** et copiez et collez le code ci-dessous, puis
    cliquez sur le bouton **Run cell**.

> **CodeCopy**
>
> à partir de pyspark.sql.functions import \*
>
> yearlySales =
> df.select(year(« OrderDate »).alias(« Année »)).groupBy(« Année »).count().orderBy(« Year »)
>
> display(annuelVentes)

![](./media/image44.png)

4.  Notez que les résultats indiquent le nombre de commandes client par
    an. Notez que la méthode **select** inclut une fonction SQL **year**
    pour extraire le composant year du champ *OrderDate* (c'est pourquoi
    le code inclut une instruction import pour importer des fonctions à
    partir de la bibliothèque SQL Spark). Il utilise ensuite une
    **méthod d'alias** pour attribuer un nom de colonne à la valeur de
    l'année extraite. Les données sont ensuite regroupées par la colonne
    dérivée *Année* et le nombre de lignes dans chaque groupe est
    calculé avant que la méthode **orderBy** ne soit utilisée pour trier
    la datatrame résultante.

![](./media/image45.png)

# Exercice 3 : Utiliser Spark pour transformer des fichiers de données

Une tâche courante des ingénieurs de données consiste à ingérer des
données dans un format ou une structure particulière, et à les
transformer pour un traitement ou une analyse ultérieure en aval.

## Tâche 1 : Utiliser des méthodes et des fonctions de dataframe pour transformer des données

1.  Cliquez sur + Code et copiez et collez le code ci-dessous

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

2.  **Exécutez (Run)** le code pour créer un nouveau dataframe à partir
    des données de commande d'origine avec les transformations suivantes
    :

    - Ajoutez les colonnes **Year** et **Month** en fonction de la
      colonne **OrderDate**.

    - Ajoutez **FirstName** et **LastName,** en fonction de la colonne
      **CustomerName**.

    - Filtrez et réorganisez les colonnes, en supprimant la colonne
      **CustomerName**.

![](./media/image47.png)

3.  Examinez la sortie et vérifiez que les modifications ont été
    apportées aux données.

![](./media/image48.png)

Vous pouvez utiliser toute la puissance de la bibliothèque SQL Spark
pour transformer les données en filtrant les lignes, en dérivant, en
supprimant, en renommant les colonnes et en appliquant toute autre
modification de données requise.

**Conseil** : Consultez [*Spark dataframe
documentation*](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html)
pour en savoir plus sur les méthodes de l'objet Dataframe.

## Tâche 2 : Enregistrer les données transformées

1.  **Add a new cell** avec le code suivant pour enregistrer le
    dataframe transformé au format Parquet (Écrasement des données si
    elles existent déjà). **Exécutez (Run)** la cellule et attendez le
    message indiquant que les données ont été enregistrées.

> CodeCopy
>
> transformed_df.write.mode(« écraser »).parquet('Fichiers/transformed_data/commandes')
>
> print (« Données transformées sauvegardées ! »)
>
> **Remarque** : En général, le *format Parquet* est préféré pour les
> fichiers de données que vous utiliserez pour une analyse plus
> approfondie ou l'ingestion dans un magasin analytique. Parquet est un
> format très efficace qui est pris en charge par la plupart des
> systèmes d'analyse de données à grande échelle. En fait, il arrive que
> votre besoin de transformation de données consiste simplement à
> convertir des données d'un autre format (tel que CSV) vers Parquet !

![](./media/image49.png)

![](./media/image50.png)

2.  Ensuite, dans le volet de **Lakehouse explorer** à gauche, dans le
    ... du nœud **Files**, sélectionnez **Refresh**.

> ![](./media/image51.png)

3.  Cliquez sur le dossier **transformed_data** pour vérifier qu'il
    contient un nouveau dossier nommé **orders**, qui contient à son
    tour un ou plusieurs **Parquet files**.

![](./media/image52.png)

4.  Cliquez sur **+ Code** suivant le code pour charger une nouvelle
    trame de données à partir des fichiers parquet dans le dossier
    **transformed_data -\> orders** :

> **CodeCopy**
>
> orders_df =
> spark.read.format(« parquet »).load(« Fichiers/transformed_data/commandes »)
>
> affichage(orders_df)
>
> ![](./media/image53.png)

5.  **Exécutez (Run)** la cellule et vérifiez que les résultats
    affichent les données de commande qui ont été chargées à partir des
    fichiers parquet.

> ![](./media/image54.png)

## Tâche 3 : Enregistrer les données dans des fichiers partitionnés

1.  Ajoutez une nouvelle cellule, cliquez sur **+ Code** avec le code
    suivant, ce qui enregistre le dataframe, en partitionnant les
    données par **year** et par **Month**. **Exécutez (Run)** la cellule
    et attendez le message indiquant que les données ont été
    enregistrées.

> CodeCopy
>
> orders_df.write.partitionBy("Year","Month").mode("overwrite").parquet("Files/partitioned_data")
>
> print ("Transformed data saved!")![](./media/image55.png)
>
> ![](./media/image56.png)

2.  Ensuite, dans le volet **Lakehouse explorer** à gauche, dans le ...
    du nœud **Files**, sélectionnez **Refresh.**

![](./media/image57.png)

![](./media/image58.png)

3.  Développez le dossier **partitioned_orders** pour vérifier qu'il
    contient une hiérarchie de dossiers nommés **Year=xxxx**, chacun
    contenant des dossiers nommés **Month=xxxx**. Chaque dossier mensuel
    contient un dossier parquet avec les commandes du mois.

![](./media/image59.png)

![](./media/image60.png)

> Le partitionnement des fichiers de données est un moyen courant
> d'optimiser les performances lors du traitement de gros volumes de
> données. Cette technique permet d'améliorer considérablement les
> performances et de faciliter le filtrage des données.

4.  Ajoutez une nouvelle cellule, cliquez sur **+ Code** avec le code
    suivant pour charger un nouveau dataframe à partir du file
    **orders.parquet** :

> CodeCopy
>
> orders_2021_df =
> spark.read.format("parquet").load("Files/partitioned_data/Year=2021/Month=\*")
>
> display(orders_2021_df)

![](./media/image61.png)

5.  **Exécutez (Run)** la cellule et vérifiez que les résultats
    affichent les données de commande pour les ventes en 2021. Notez que
    les colonnes de partitionnement spécifiées dans le chemin d'accès
    (**(Year and Month)**) ne sont pas incluses dans la trame de
    données.

![](./media/image62.png)

# **Exercice 3 : Utilisation des tables et de SQL**

Comme vous l'avez vu, les méthodes natives de l'objet dataframe vous
permettent d'interroger et d'analyser les données d'un fichier de
manière assez efficace. Cependant, de nombreux analystes de données sont
plus à l'aise avec des tables qu'ils peuvent interroger à l'aide de la
syntaxe SQL. Spark fournit un *metastore* dans lequel vous pouvez
définir des tables relationnelles. La bibliothèque SQL Spark qui fournit
l'objet dataframe prend également en charge l'utilisation d'instructions
SQL pour interroger des tables dans le metastore. En utilisant ces
fonctionnalités de Spark, vous pouvez combiner la flexibilité d'un lac
de données avec le schéma de données structurées et les requêtes SQL
d'un entrepôt de données relationnel, d'où le terme « data lakehouse ».

## Tâche 1 : Créer une table gérée

Les tables d'un metastore Spark sont des abstractions relationnelles sur
les fichiers du lac de données. Les tables peuvent être *gérées* (auquel
cas les fichiers sont gérés par le metastore) ou *externes* (auquel cas
la table fait référence à un emplacement de fichier dans le lac de
données que vous gérez indépendamment du metastore).

1.  Ajoutez un nouveau code, cliquez sur la cellule **+ Code** dans le
    carnet et entrez le code suivant, qui enregistre le dataframe des
    données de commande client sous la forme d'une table nommée
    **salesorders** :

> CodeCopy
>
> \# Create a new table
>
> df.write.format("delta").saveAsTable("salesorders")
>
> \# Get the table description
>
> spark.sql("DESCRIBE EXTENDED salesorders").show(truncate=False)

![Une capture d'écran d'un ordinateur Description générée
automatiquement](./media/image63.png)

**Remarque** : Il convient de noter deux ou trois choses à propos de cet
exemple. Tout d'abord, aucun chemin explicite n'est fourni, de sorte que
les fichiers de la table seront gérés par le metastore. Deuxièmement, la
table est enregistrée au format **delta**. Vous pouvez créer des tables
basées sur plusieurs formats de fichiers (notamment CSV, Parquet, Avro,
etc.), mais *delta lake* est une technologie Spark qui ajoute des
fonctionnalités de base de données relationnelle aux tables, y compris
la prise en charge des transactions, la gestion des versions de ligne et
d'autres fonctionnalités utiles. La création de tables au format delta
est recommandée pour les lakehouses de données dans Fabric.

2.  **Exécutez (Run)** la cellule de code et examinez la sortie, qui
    décrit la définition de la nouvelle table.

![Une capture d'écran d'un ordinateur Description générée
automatiquement](./media/image64.png)

3.  Dans le volet **Lakehouse explorer**, dans le ... du dossier
    **Tables**, sélectionnez **Refresh.**

![Une capture d'écran d'un ordinateur Description générée
automatiquement](./media/image65.png)

4.  Ensuite, développez le nœud **Tables** et vérifiez que la table
    **salesorders** a été créée.

> ![Une capture d'écran d'un ordinateur Description générée
> automatiquement](./media/image66.png)

5.  Passez votre souris à côté du **salesorders**, puis cliquez sur les
    points de suspension horizontaux (...). Naviguez et cliquez sur
    **Load data**, puis sélectionnez **Spark**.

![Une capture d'écran d'un ordinateur Description générée
automatiquement](./media/image67.png)

6.  Cliquez sur le bouton **▷ Run cell** et qui utilise la bibliothèque
    SQL Spark pour intégrer une requête SQL sur la table de
    **salesorder** dans le code PySpark et charger les résultats de la
    requête dans un dataframe.

> CodeCopy
>
> df = spark.sql(« SELECT \* FROM \[your_lakehouse\].salesorders LIMIT
> 1000 »)
>
> Affichage(DF)

![Une capture d'écran d'un programme informatique Description générée
automatiquement](./media/image68.png)

![](./media/image69.png)

## Tâche 2 : Créer une table externe

Vous pouvez également créer des *tables externes* pour lesquelles les
métadonnées de schéma sont définies dans le metastore du lakehouse, mais
les fichiers de données sont stockés dans un emplacement externe.

1.  Sous les résultats renvoyés par la première cellule de code,
    utilisez le bouton **+ Code** pour ajouter une nouvelle cellule de
    code si elle n'en existe pas déjà. Entrez ensuite le code suivant
    dans la nouvelle cellule.

CodeCopy

> df.write.format("delta").saveAsTable("external_salesorder",
> path="\<abfs_path\>/external_salesorder")

![Une capture d'écran d'un ordinateur Description générée
automatiquement](./media/image70.png)

2.  Dans le volet de **Lakehouse explorer**, dans le ... du dossier
    **Files**, sélectionnez **Copy ABFS path** dans le Notebook.

> Le chemin ABFS est le chemin d'accès complet au dossier **Files** dans
> le stockage OneLake de votre Lakehouse - similaire à ceci :

abfss://dp_Fabric29@onelake.dfs.fabric.microsoft.com/Fabric_lakehouse.Lakehouse/Files/external_salesorder

![Une capture d'écran d'un ordinateur Description générée
automatiquement](./media/image71.png)

3.  Maintenant, déplacez-vous dans la cellule de code,
    remplacez**-\<abfs_path\>** par le **chemin d’accès (path)** vous
    avez copié dans le Notebook afin que le code enregistre le cadre de
    données en tant que table externe avec des fichiers de données dans
    un dossier nommé **external_salesorder** dans votre emplacement de
    dossier Fichiers. Le chemin d'accès complet doit ressembler à ceci

abfss://dp_Fabric29@onelake.dfs.fabric.microsoft.com/Fabric_lakehouse.Lakehouse/Files/external_salesorder

4.  Utilisez le bouton **▷ (*Run cell*)** à gauche de la cellule pour
    l'exécuter.

![](./media/image72.png)

5.  Dans le volet de **Lakehouse explorer**, dans le ... menu du dossier
    **Tables**, sélectionnez l'option **Refresh**.

![Une capture d'écran d'un ordinateur Description générée
automatiquement](./media/image73.png)

6.  Développez ensuite le nœud **Tables** et vérifiez que la table
    **external_salesorder** a été créée.

![Une capture d'écran d'un ordinateur Description générée
automatiquement](./media/image74.png)

7.  Dans le volet de **Lakehouse** **explorer**, dans le ... menu du
    dossier **Files**, sélectionnez **Refresh**.

![Une capture d'écran d'un ordinateur Description générée
automatiquement](./media/image75.png)

8.  Développez ensuite le nœud **Files** et vérifiez que le dossier
    **external_salesorder** a été créé pour les fichiers de données de
    la table.

![Une capture d'écran d'un ordinateur Description générée
automatiquement](./media/image76.png)

## Tâche 3 : Comparer les tables gérées et externes

Explorons les différences entre les tables managées et externes.

1.  Sous les résultats renvoyés par la cellule de code, utilisez le
    bouton **+ Code** pour ajouter une nouvelle cellule de code. Copiez
    le code ci-dessous dans la cellule Code et utilisez le bouton **▷
    (*Run cell*)** à gauche de la cellule pour l'exécuter.

> SqlCopy
>
> %%sql

DESCRIBE FORMATTED salesorders;

![Une capture d'écran d'un ordinateur Description générée
automatiquement](./media/image77.png)

![Une capture d'écran d'un ordinateur Description générée
automatiquement](./media/image78.png)

2.  Dans les résultats, affichez la propriété **Location** de la table,
    qui doit être un chemin d'accès au stockage OneLake pour le
    lakehouse se terminant par **/Tables/salesorders** (vous devrez
    peut-être élargir la colonne **Data Type** pour voir le chemin
    d'accès complet).

![Une capture d'écran d'un ordinateur Description générée
automatiquement](./media/image79.png)

3.  Modifiez la commande **DESCRIBE** pour afficher les détails de la
    table **external_saleorder**, comme illustré ici.

4.  Sous les résultats renvoyés par la cellule de code, utilisez le
    bouton **+ Code** pour ajouter une nouvelle cellule de code. Copiez
    le code ci-dessous et utilisez le bouton **▷ (*Run cell*)** à gauche
    de la cellule pour l'exécuter.

> SqlCopy
>
> %%sql
>
> DESCRIBE FORMATTED external_salesorder;

![Une capture d'écran d'un e-mail Description générée
automatiquement](./media/image80.png)

5.  Dans les résultats, affichez la propriété **Location** de la table,
    qui doit être un chemin d'accès au stockage OneLake pour le
    lakehouse se terminant par **/Files/external_saleorder** (vous
    devrez peut-être élargir la colonne **Data Type** pour voir le
    chemin d'accès complet).

![Une capture d'écran d'un ordinateur Description générée
automatiquement](./media/image81.png)

## Tâche 4 : Exécuter du code SQL dans une cellule

Bien qu'il soit utile de pouvoir intégrer des instructions SQL dans une
cellule contenant du code PySpark, les analystes de données souhaitent
souvent travailler directement en SQL.

1.  Cliquez sur la cellule **+ Code** du carnet et entrez le code
    suivant dans celui-ci. Cliquez sur **l**e bouton **▷ Run cell** et
    examinez les résultats. Observez que :

    - La ligne %%sql au début de la cellule (appelée *magie*) indique
      que le runtime du langage SQL Spark doit être utilisé pour
      exécuter le code dans cette cellule à la place de PySpark.

    - Le code SQL fait référence à la table **salesorders** que vous
      avez créée précédemment.

    - La sortie de la requête SQL s'affiche automatiquement en tant que
      résultat sous la cellule

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

![Une capture d'écran d'un ordinateur Description générée
automatiquement](./media/image82.png)

![](./media/image83.png)

**Remarque** : Pour plus d'informations sur Spark SQL et les dataframes,
consultez la [*documentation Spark
SQL*](https://spark.apache.org/docs/2.2.0/sql-programming-guide.html).

# Exercice 4 : Visualiser des données avec Spark

Une image vaut proverbialement mille mots, et un graphique vaut souvent
mieux qu'un millier de lignes de données. Bien que les notebooks de
Fabric incluent une vue graphique intégrée pour les données affichées à
partir d'une trame de données ou d'une requête SQL Spark, elle n'est pas
conçue pour la création de graphiques complète. Cependant, vous pouvez
utiliser des bibliothèques graphiques Python telles que **matplotlib**
et **seaborn** pour créer des graphiques à partir de données dans des
dataframes.

## Tâche 1 : Afficher les résultats sous forme de graphique

1.  Cliquez sur la cellule **+ Code** du carnet et entrez le code
    suivant dans celui-ci. Cliquez sur le bouton **▷ Run cell** et
    observez qu'il renvoie les données de la vue **salesorders** que
    vous avez créée précédemment.

> SqlCopy
>
> %%sql
>
> SELECT \* FROM salesorders

![Une capture d'écran d'un ordinateur Description générée
automatiquement](./media/image84.png)

![Une capture d'écran d'un ordinateur Description générée
automatiquement](./media/image85.png)

2.  Dans la section des résultats sous la cellule, remplacez l’option
    **View** de **Table** à **Chart**.

![Une capture d'écran d'un ordinateur Description générée
automatiquement](./media/image86.png)

3.  Utilisez le bouton **View options** en haut à droite du graphique
    pour afficher le volet des options du graphique. Définissez ensuite
    les options comme suit et sélectionnez **Apply** :

    - **Chart type**: Bar chart

    - **Key**: Item

    - **Values**: Quantity

    - **Series Group**: *leave blank*

    - **Aggregation**: Sum

    - **Stacked**: *Non sélectionné*

![Un code-barres bleu sur fond blanc Description générée
automatiquement](./media/image87.png)

![Une capture d'écran d'un graphique Description générée
automatiquement](./media/image88.png)

4.  Vérifiez que le graphique ressemble à ceci

![Une capture d'écran d'un ordinateur Description générée
automatiquement](./media/image89.png)

## Tâche 2 : Démarrer avec matplotlib

1.  Cliquez sur **+ Code** et copiez et collez le code ci-dessous.
    **Exécutez (Run)** le code et observez qu'il retourne une trame de
    données Spark contenant le chiffre d'affaires annuel.

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

![Une capture d'écran d'un ordinateur Description générée
automatiquement](./media/image90.png)

![](./media/image91.png)

2.  Pour visualiser les données sous forme de graphique, nous allons
    commencer par utiliser la bibliothèque **Python matplotlib**. Cette
    bibliothèque est la bibliothèque de traçage de base sur laquelle de
    nombreuses autres sont basées, et offre une grande flexibilité dans
    la création de graphiques.

3.  Cliquez sur **+ Code** et copiez et collez le code ci-dessous.

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

![Une capture d'écran d'un ordinateur Description générée
automatiquement](./media/image92.png)

5.  Cliquez sur le bouton **Run cell** et examinez les résultats, qui
    consistent en un histogramme avec le revenu brut total pour chaque
    année. Notez les caractéristiques suivantes du code utilisé pour
    produire ce graphique :

    - La bibliothèque **matplotlib** nécessite une trame *de données
      Pandas*, vous devez donc convertir la trame de données *Spark*
      renvoyée par la requête SQL Spark dans ce format.

    - Au cœur de la bibliothèque **matplotlib** se trouve l'objet
      **pyplot**. C'est la base de la plupart des fonctionnalités de
      traçage.

    - Les paramètres par défaut permettent d'obtenir un graphique
      utilisable, mais il est possible de le personnaliser

![Une capture d'écran d'un écran d'ordinateur Description générée
automatiquement](./media/image93.png)

6.  Modifiez le code pour tracer le graphique comme suit, remplacez tout
    le code de la **cell** par le code suivant et cliquez sur le bouton
    **▷ Run cell** et vérifiez la sortie

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
> plt.show()
>
> ![Une capture d'écran d'un graphique Description générée
> automatiquement](./media/image94.png)

7.  Le tableau comprend maintenant un peu plus d'informations. Un
    complot est techniquement contenu par une **figure**. Dans les
    exemples précédents, la figure a été créée implicitement pour vous ;
    Mais vous pouvez le créer explicitement.

8.  Modifiez le code pour tracer le graphique comme suit, remplacez tout
    le code de la **cell** par le code suivant.

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
> plt.show()![Une capture d'écran d'un programme informatique
> Description générée automatiquement](./media/image95.png)

9.  **Re-run** la cellule de code et affichez les résultats. La figure
    détermine la forme et la taille du tracé.

> Une figure peut contenir plusieurs sous-tracés, chacun sur son propre
> *axe*.

![Une capture d'écran d'un ordinateur Description générée
automatiquement](./media/image96.png)

10. Modifiez le code pour tracer le graphique comme suit. **Re-run** la
    cellule de code et affichez les résultats. La figure contient les
    sous-parcelles spécifiées dans le code.

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

![Une capture d'écran d'un programme informatique Description générée
automatiquement](./media/image97.png)

![](./media/image98.png)

**Remarque** : Pour en savoir plus sur le traçage avec matplotlib,
consultez la [*documentation de matplotlib*](https://matplotlib.org/).

## Tâche 3 : Utiliser la bibliothèque seaborn

Bien que **matplotlib** vous permette de créer des graphiques complexes
de plusieurs types, il peut nécessiter un code complexe pour obtenir les
meilleurs résultats. Pour cette raison, au fil des ans, de nombreuses
nouvelles bibliothèques ont été construites sur la base de matplotlib
pour abstraire sa complexité et améliorer ses capacités. L'une de ces
bibliothèques est **seaborn**.

1.  Cliquez sur **+ Code** et copiez et collez le code ci-dessous.

CodeCopy

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

![Une capture d'écran d'un graphique Description générée
automatiquement](./media/image99.png)

2.  **Exécutez (Run)** le code et observez qu'il affiche un graphique à
    barres à l'aide de la bibliothèque seaborn.

![Une capture d'écran d'un graphique Description générée
automatiquement](./media/image100.png)

3.  **Modify** le code comme suit. **Exécutez (Run)** le code modifié et
    notez que seaborn vous permet de définir un thème de couleur
    cohérent pour vos parcelles.

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

4.  **Modify** à nouveau le code comme suit. **Exécutez (Run)** le code
    modifié pour afficher le chiffre d'affaires annuel sous forme de
    graphique linéaire.

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
>
> ![](./media/image102.png)

**Remarque** : Pour en savoir plus sur le traçage avec seaborn,
consultez la [*documentation
seaborn*](https://seaborn.pydata.org/index.html).

## Tâche 4 : Utiliser des tables delta pour la diffusion en continu de données

Le lac Delta prend en charge les données en continu. Les tables delta
peuvent être un *récepteur* ou une *source* pour les flux de données
créés à l'aide de l'API Spark Structured Streaming. Dans cet exemple,
vous allez utiliser une table delta comme récepteur pour certaines
données en streaming dans un scénario IoT (Internet des objets) simulé.

1.  Cliquez sur **+ Code** et copiez et collez le code ci-dessous, puis
    cliquez sur le bouton **Run cell**.

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

print("Source stream created...")![Une capture d'écran d'un programme
informatique Description générée automatiquement](./media/image103.png)

![Une capture d'écran d'un ordinateur Description générée
automatiquement](./media/image104.png)

2.  Assurez-vous que le message ***Source stream created…*** est
    imprimé. Le code que vous venez d'exécuter a créé une source de
    données de streaming basée sur un dossier dans lequel certaines
    données ont été enregistrées, représentant des lectures d'appareils
    IoT hypothétiques.

3.  Cliquez sur **+ Code** et copiez et collez le code ci-dessous, puis
    cliquez sur le bouton **Run cell**.

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
>
> ![Une capture d'écran d'un ordinateur Description générée
> automatiquement](./media/image105.png)

4.  Ce code écrit les données de l'appareil de streaming au format delta
    dans un dossier nommé **iotdevicedata**. Étant donné que le chemin
    d'accès à l'emplacement du dossier se trouve dans le dossier
    **Tables**, une table sera automatiquement créée pour celui-ci.
    Cliquez sur les points de suspension horizontaux à côté du tableau,
    puis cliquez sur **Refresh**.

![](./media/image106.png)

![Une capture d'écran d'un ordinateur Description générée
automatiquement](./media/image107.png)

5.  Cliquez sur **+ Code** et copiez et collez le code ci-dessous, puis
    cliquez sur le bouton **Run cell**.

> SqlCopy
>
> %%sql
>
> SELECT \* FROM IotDeviceData;

![Une capture d'écran d'un ordinateur Description générée
automatiquement](./media/image108.png)

6.  Ce code interroge la table **IotDeviceData**, qui contient les
    données de l'appareil à partir de la source de streaming.

7.  Cliquez sur **+ Code** et copiez et collez le code ci-dessous, puis
    cliquez sur le bouton **Run cell**.

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

![Une capture d'écran d'un ordinateur Description générée
automatiquement](./media/image109.png)

8.  Ce code écrit d'autres données d'appareil hypothétiques dans la
    source de streaming.

9.  Cliquez sur **+ Code** et copiez et collez le code ci-dessous, puis
    cliquez sur le bouton **Run cell**.

> SqlCopy
>
> %%sql
>
> SELECT \* FROM IotDeviceData;
>
> ![Une capture d'écran d'un ordinateur Description générée
> automatiquement](./media/image110.png)

10. Ce code interroge à nouveau la table **IotDeviceData**, qui doit
    maintenant inclure les données supplémentaires qui ont été ajoutées
    à la source de streaming.

11. Cliquez sur **+ Code** et copiez et collez le code ci-dessous, puis
    cliquez sur le bouton **Run cell**.

> CodeCopy
>
> deltastream.stop()

![Une capture d'écran d'un ordinateur Description générée
automatiquement](./media/image111.png)

12. Ce code arrête le flux.

## Tâche 5 : Enregistrer le Notebook et terminer la session Spark

Maintenant que vous avez terminé d'utiliser les données, vous pouvez
enregistrer le Notebook sous un nom significatif et mettre fin à la
session Spark.

1.  Dans la barre de menus du Notebook, utilisez l'⚙️ icône **Settings**
    pour afficher les paramètres du Notebook.

![Une capture d'écran d'un ordinateur Description générée
automatiquement](./media/image112.png)

2.  Définissez le **Name** du Notebook sur ++ **Explore Sales Orders**
    ++, puis fermez le volet des paramètres.

![Une capture d'écran d'un ordinateur Description générée
automatiquement](./media/image113.png)

3.  Dans le menu du Notebook, sélectionnez **Stop session** pour mettre
    fin à la session Spark.

![Une capture d'écran d'un ordinateur Description générée
automatiquement](./media/image114.png)

![Une capture d'écran d'un ordinateur Description générée
automatiquement](./media/image115.png)

# Exercice 5 : Création d'un flux de données (Gen2) dans Microsoft Fabric

Dans Microsoft Fabric, les flux de données (Gen2) se connectent à
diverses sources de données et effectuent des transformations dans Power
Query Online. Ils peuvent ensuite être utilisés dans les pipelines de
données pour ingérer des données dans un lakehouse ou un autre magasin
analytique, ou pour définir un jeu de données pour un rapport Power BI.

Cet exercice est conçu pour présenter les différents éléments de
Dataflows (Gen2), et non pour créer une solution complexe qui peut
exister dans une entreprise

## Tâche 1 : Créer un flux de données (Gen2) pour ingérer des données

Maintenant que vous avez un Lakehouse, vous devez y ingérer des données.
Pour ce faire, vous devez définir un flux de données qui encapsule un
processus d'*extraction, de transformation et de chargement* (ETL).

1.  Maintenant, cliquez sur **Fabric_lakehouse** dans le volet de
    navigation de gauche.

![Une capture d'écran d'un ordinateur Description générée
automatiquement](./media/image116.png)

2.  Sur la page d'accueil **Fabric_lakehouse**, cliquez sur la flèche
    déroulante dans **Get data** et sélectionnez **New Dataflow Gen2.**
    L'éditeur Power Query de votre nouveau flux de données s'ouvre.

![](./media/image117.png)

3.  Dans le **volet Power Query**, sous l'onglet **Home**, cliquez sur
    **Import from a Text/CSV file**

![](./media/image118.png)

4.  Dans le volet **Connect to data source**, sous **Connection
    settings**, sélectionnez le bouton de radio **Link to file
    (Preview)**

- **Link to file** : *Sélectionné*

- **File path or URL**:
  <https://raw.githubusercontent.com/MicrosoftLearning/dp-data/main/orders.csv>

![](./media/image119.png)

5.  Dans le volet **Connect to data source**, sous **Connection
    credentials,** entrez les détails suivants et cliquez sur le bouton
    **Next**.

    - **Connection**: Create new connection

    - **data gateway**: (none)

    - **Authentication kind**: Organizational account

![](./media/image120.png)

6.  Dans **Preview file data**, cliquez sur **Create** pour créer la
    source de données. ![Une capture d'écran d'un ordinateur Description
    générée automatiquement](./media/image121.png)

7.  L'éditeur **Power Query** affiche la source de données et un
    ensemble initial d'étapes de requête pour mettre en forme les
    données.

![Une capture d'écran d'un ordinateur Description générée
automatiquement](./media/image122.png)

8.  Dans le ruban de la barre d'outils, sélectionnez l'onglet **Add
    column**. Ensuite, sélectionnez **Custom column.**

![Une capture d'écran d'un ordinateur Description générée
automatiquement](./media/image123.png) 

9.  Définissez le nom de la nouvelle colonne sur **MonthNo** ,
    définissez le type de données sur **Whole Number**, puis ajoutez la
    formule suivante :**Date.Month(\[OrderDate\])** sous **Custom column
    formula**. Sélectionnez **OK**.

![Une capture d'écran d'un ordinateur Description générée
automatiquement](./media/image124.png)

10. Notez comment l'étape d'ajout de la colonne personnalisée est
    ajoutée à la requête. La colonne résultante s'affiche dans le volet
    de données.

![Une capture d'écran d'un ordinateur Description générée
automatiquement](./media/image125.png)

**Conseil :** Dans le volet Paramètres de requête sur le côté droit,
notez que les **Applied Steps** incluent chaque étape de transformation.
En bas, vous pouvez également basculer le bouton **Diagram flow** pour
activer le diagramme visuel des étapes.

Les étapes peuvent être déplacées vers le haut ou vers le bas, modifiées
en sélectionnant l'icône d'engrenage, et vous pouvez sélectionner chaque
étape pour voir les transformations s'appliquer dans le volet d'aperçu.

Tâche 2 : Ajouter une destination de données pour Dataflow

1.  Dans le ruban de la barre d'outils **Power Query, sélectionnez
    l**'onglet **Home**. Ensuite, dans le menu déroulant **Data
    destination**, sélectionnez **Lakehouse** (si ce n'est pas déjà
    fait).

![](./media/image126.png)

![Une capture d'écran d'un ordinateur Description générée
automatiquement](./media/image127.png)

**Remarque :** Si cette option est grisée, il se peut qu'une destination
de données soit déjà définie. Vérifiez la destination des données en bas
du volet Paramètres de requête sur le côté droit de l'éditeur Power
Query. Si une destination est déjà définie, vous pouvez la modifier à
l'aide de l'engrenage.

2.  Cliquez sur l’icône **Settings** à côté de l'option **Lakehouse**
    sélectionnée.

![Une capture d'écran d'un ordinateur Description générée
automatiquement](./media/image128.png)

3.  Dans la boîte de dialogue **Connect to data destination,
    sélectionnez Edit la connection.**

![Une capture d'écran d'un ordinateur Description générée
automatiquement](./media/image129.png)

4.  Dans la boîte de dialogue **Connect to data destination,**
    sélectionnez **Sign** à l'aide de votre compte d'organisation Power
    BI pour définir l'identité que le flux de données utilise pour
    accéder au lakehouse.

![Une capture d'écran d'un ordinateur Description générée
automatiquement](./media/image130.png)

![Une capture d'écran d'un ordinateur Description générée
automatiquement](./media/image131.png)

5.  Dans la boîte de dialogue Se connecter à la destination des données,
    sélectionnez **Next**

![Une capture d'écran d'un ordinateur Description générée
automatiquement](./media/image132.png)

6.  Dans la boîte de dialogue Se connecter à la destination des données,
    sélectionnez **New table**. Cliquez sur le dossier **Lakehouse**,
    sélectionnez votre espace de travail (workspace) dp_FabricXX puis
    sélectionnez votre lakehouse, c'est-à-dire **Fabric_lakehouse.**
    Spécifiez ensuite le nom de la table sous forme **orders** et
    sélectionnez le bouton **Next**.

![Une capture d'écran d'un ordinateur Description générée
automatiquement](./media/image133.png)

7.  Dans la boîte de dialogue **Choose destination settings,** sous
    **Use automatic settings off** et **Update method,** sélectionnez
    **Apprend**, puis cliquez sur le bouton **Save settings**.

![](./media/image134.png)

8.  La destination **Lakehouse** est indiquée sous forme d’icône dans la
    **query** dans l'éditeur Power Query.

![Une capture d'écran d'un ordinateur Description générée
automatiquement](./media/image135.png)

![Une capture d'écran d'un ordinateur Description générée
automatiquement](./media/image136.png)

9.  Sélectionnez **Publish** pour publier le flux de données. Attendez
    ensuite que le flux de données **Dataflow 1** soit créé dans votre
    espace de travail (workspace).

![Une capture d'écran d'un ordinateur Description générée
automatiquement](./media/image137.png)

10. Une fois publié, vous pouvez cliquer avec le bouton droit sur le
    flux de données dans votre espace de travail (workspace),
    sélectionner **Propriétés** et renommer votre flux de données.

![Une capture d'écran d'un ordinateur Description générée
automatiquement](./media/image138.png)

11. Dans la boîte de dialogue **Dataflow1**, saisissez le **Name** sous
    la forme **Gen2_Dataflow** puis cliquez sur le bouton **Save**.

![Une capture d'écran d'un ordinateur Description générée
automatiquement](./media/image139.png)

![](./media/image140.png)

## Tâche 3 : Ajouter un flux de données à un pipeline

Vous pouvez inclure un flux de données en tant qu'activité dans un
pipeline. Les pipelines sont utilisés pour orchestrer les activités
d'ingestion et de traitement des données, ce qui vous permet de combiner
des flux de données avec d'autres types d'opérations dans un seul
processus planifié. Les pipelines peuvent être créés dans différentes
expériences, y compris l'expérience Data Factory.

1.  Sur la page d'accueil de Synapse Data Engineering, sous **le volet
    dp_FabricXX** , sélectionnez **+New item** -\> **Data** **Pipeline**

![](./media/image141.png)

2.  Dans la boîte de dialogue **New pipeline**, entrez **Load data** les
    données dans le champ **Name**, cliquez sur le bouton Créer pour
    ouvrir le nouveau pipeline.

![Une capture d'écran d'un ordinateur Description générée
automatiquement](./media/image142.png)

3.  L'éditeur de pipeline s'ouvre.

![Une capture d'écran d'un ordinateur Description générée
automatiquement](./media/image143.png)

> **Astuce** : Si l'assistant de copie de données s'ouvre
> automatiquement, fermez-le !

4.  Sélectionnez **Pipeline activity**, puis ajoutez une activité de
    **Dataflow** au pipeline.

![](./media/image144.png)

5.  Une fois la nouvelle activité **Dataflow1** sélectionnée, sous
    l'onglet **Settings**, dans la liste déroulante Flux de données,
    sélectionnez **Gen2_Dataflow** (le flux de données que vous avez
    créé précédemment)

![](./media/image145.png)

6.  Sous l'onglet **Home**, enregistrez le pipeline à l'aide de l'**🖫**
    icône ***(*Save**).

![Une capture d'écran d'un ordinateur Description générée
automatiquement](./media/image146.png)

7.  Utilisez le bouton **▷ Run** pour exécuter le pipeline et attendez
    qu'il se termine. Cela peut prendre quelques minutes.

> ![Une capture d'écran d'un ordinateur Description générée
> automatiquement](./media/image147.png)
>
> ![Une capture d'écran d'un ordinateur Description générée
> automatiquement](./media/image148.png)

![Une capture d'écran d'un ordinateur Description générée
automatiquement](./media/image149.png)

8.  Dans la barre de menu sur le bord gauche, sélectionnez votre espace
    de travail (workspace), c'est-à-dire **dp_FabricXX**.

![Une capture d'écran d'un ordinateur Description générée
automatiquement](./media/image150.png)

9.  Dans le volet **Fabric_lakehouse**, sélectionnez le
    **Gen2_FabricLakehouse** de type Lakehouse.

![Une capture d'écran d'un ordinateur Description générée
automatiquement](./media/image151.png)

10. Dans le volet **Explorer**, sélectionnez l'icône **...** pour
    **Tables**, sélectionnez **Refresh**. Développez ensuite **Tables**
    et sélectionnez la table **orders** qui a été créée par votre flux
    de données.

![Une capture d'écran d'un ordinateur Description générée
automatiquement](./media/image152.png)

![](./media/image153.png)

**Conseil** : Utilisez le connecteur Power BI Desktop *Dataflows* pour
vous connecter directement aux transformations de données effectuées
avec votre flux de données.

Vous pouvez également effectuer des transformations supplémentaires,
publier en tant que nouveau jeu de données et distribuer auprès du
public cible des jeux de données spécialisés.

## Tâche 4 : Nettoyer les ressources

Dans cet exercice, vous allez apprendre à utiliser Spark pour utiliser
des données dans Microsoft Fabric.

Si vous avez terminé d'explorer votre lakehouse, vous pouvez supprimer
l'espace de travail (workspace) que vous avez créé pour cet exercice.

1.  Dans la barre de gauche, sélectionnez l'icône de votre espace de
    travail (workspace) pour afficher tous les éléments qu'il contient.

> ![Une capture d'écran d'un ordinateur Description générée
> automatiquement](./media/image154.png)

2.  Dans le **...** dans la barre d'outils, sélectionnez **Workspace
    setting**.

![](./media/image155.png)

3.  Sélectionnez **General** et cliquez sur **Remove this workspace.**

![Une capture d'écran des paramètres d'un ordinateur Description générée
automatiquement](./media/image156.png)

4.  Dans **Delete workspace?** cliquez sur le bouton **Delete**.

> ![Une capture d'écran d'un ordinateur Description générée
> automatiquement](./media/image157.png)
>
> ![Une capture d'écran d'un ordinateur Description générée
> automatiquement](./media/image158.png)

**Résumé**

Ce cas d'utilisation vous guide tout au long du processus d'utilisation
de Microsoft Fabric dans Power BI. Il couvre diverses tâches, notamment
la configuration d'un workspace, la création d'un lakehouse, le
téléchargement et la gestion de fichiers de données et l'utilisation de
carnets de notes pour l'exploration des données. Les participants
apprendront à manipuler et à transformer des données à l'aide de
PySpark, à créer des visualisations, ainsi qu'à enregistrer et
partitionner des données pour des requêtes efficaces.

Dans ce cas d'utilisation, les participants s'engageront dans une série
de tâches axées sur l'utilisation des tables delta dans Microsoft
Fabric. Les tâches comprennent le téléchargement et l'exploration de
données, la création de tables delta gérées et externes, la comparaison
de leurs propriétés, l'introduction de fonctionnalités SQL pour la
gestion des données structurées et la fourniture d'informations sur la
visualisation des données à l'aide de bibliothèques Python telles que
matplotlib et seaborn. Les exercices visent à fournir une compréhension
complète de l'utilisation de Microsoft Fabric pour l'analyse des données
et de l'intégration de tables delta pour la diffusion de données en
continu dans un contexte IoT.

Ce cas d'utilisation vous guide tout au long du processus de
configuration d'un workspace Fabric, de création d'un data lakehouse et
d'ingestion de données à des fins d'analyse. Il montre comment définir
un flux de données pour gérer les opérations ETL et configurer les
destinations de données pour le stockage des données transformées. En
outre, vous apprendrez à intégrer le flux de données dans un pipeline
pour un traitement automatisé. Enfin, vous recevrez des instructions
pour nettoyer les ressources une fois l'exercice terminé.

Cet atelier vous permet d'acquérir des compétences essentielles pour
travailler avec Fabric, ce qui vous permet de créer et de gérer des
espaces de travail, d'établir des lakehouses de données et d'effectuer
efficacement des transformations de données. En intégrant des flux de
données dans des pipelines, vous apprendrez à automatiser les tâches de
traitement des données, à rationaliser votre flux de travail et à
améliorer la productivité dans des scénarios réels. Les instructions de
nettoyage vous permettent de ne laisser aucune ressource inutile, ce qui
favorise une approche de gestion de l'espace de travail (workspace)
organisée et efficace.
