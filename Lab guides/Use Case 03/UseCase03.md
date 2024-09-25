Use Case 03: Data Factory solution for moving and transforming data with
dataflows and data pipelines

**Introduction**

This lab helps you accelerate the evaluation process for Data Factory in
Microsoft Fabric by providing a step-by-step guidance for a full data
integration scenario within one hour. By the end of this tutorial, you
understand the value and key capabilities of Data Factory and know how
to complete a common end-to-end data integration scenario.

**Objective**

The lab is divided into three modules:

- Exercise 1: Create a pipeline with Data Factory to ingest raw data
  from a Blob storage to a Bronze table in a data Lakehouse.

- Exercise 2: Transform data with a dataflow in Data Factory to process
  the raw data from your Bronze table and move it to a Gold table in the
  data Lakehouse.

- Exercise 3: Automate and send notifications with Data Factory to send
  an email to notify you once all the jobs are complete, and finally,
  setup the entire flow to run on a scheduled basis.

# Exercise 1: Create a pipeline with Data Factory

** Important**

Microsoft Fabric is currently in PREVIEW. This information relates to a
prerelease product that may be substantially modified before it's
released. Microsoft makes no warranties, expressed or implied, with
respect to the information provided here. Refer to [**<u>Azure Data
Factory
documentation</u>**](https://learn.microsoft.com/en-us/azure/data-factory/) for
the service in Azure.

## Task 1: Create a workspace

Before working with data in Fabric, create a workspace with the Fabric
trial enabled.

1.  Open your browser, navigate to the address bar, and type or paste
    the following URL: <https://app.fabric.microsoft.com/> then press
    the **Enter** button.

> <img src="./media/image1.png" style="width:4.76705in;height:2.87034in"
> alt="A screenshot of a computer Description automatically generated" />
>
> **Note**: If you are directed to Microsoft Fabric Home page, then skip
> steps from \#2 to \#4.

2.  In the **Microsoft Fabric** window, enter your credentials, and
    click on the **Submit** button.

> <img src="./media/image2.png" style="width:6.49167in;height:3.11667in"
> alt="A close up of a white and green object Description automatically generated" />

3.  Then, in the **Microsoft** window, enter the password and click on
    the **Sign in** button**.**

> <img src="./media/image3.png" style="width:3.9375in;height:3.34797in"
> alt="A login screen with a red box and blue text Description automatically generated" />

4.  In **Stay signed in?** window, click on the **Yes** button.

> <img src="./media/image4.png" style="width:4.34583in;height:3.47667in"
> alt="A screenshot of a computer error Description automatically generated" />

5.  In the **Microsoft Fabric** home page, select the **Power BI**
    template.

> <img src="./media/image5.png" style="width:6.49167in;height:4.20833in"
> alt="A screenshot of a computer Description automatically generated" />

6.  In the **Power BI Home** page left-sided navigation bar,
    select **Workspaces** (the icon looks similar to 🗇).

> <img src="./media/image6.png" style="width:6.5in;height:6.23333in"
> alt="A screenshot of a computer Description automatically generated" />

7.  In the Workspaces pane, select **+** **New workspace**.

> <img src="./media/image7.png" style="width:3.5593in;height:6.60417in"
> alt="A screenshot of a computer Description automatically generated" />

8.  In the **Create a workspace** tab, enter the following details and
    click on the **Apply** button.

| **Name** | ***D**ata-FactoryXX (*XX can be a unique number) (here, we entered ***Dataflow \_Fabric29)*** |
|----|----|
| **Advanced** | Under **License mode**, select **Trial** |
| **Default storage format** | **Small dataset storage format** |

> <img src="./media/image8.png"
> style="width:5.02821in;height:6.52917in" />

<img src="./media/image9.png" style="width:5.25417in;height:6.23887in"
alt="A screenshot of a computer Description automatically generated" />

<img src="./media/image10.png"
style="width:4.02917in;height:4.92138in" />

9.  Wait for the deployment to complete. It’ll take approximately 2-3
    minutes.

10. In the **Data-FactoryXX** workspace page, navigate and click on
    **+New** button, then select **Lakehouse.**

<img src="./media/image11.png"
style="width:6.49167in;height:4.57917in" />

11. In the **New lakehouse** dialog box, enter
    +++**DataFactoryLakehouse+++** in the **Name** field, click on the
    **Create** button and open the new lakehouse.

<img src="./media/image12.png"
style="width:2.99167in;height:1.51667in" />

<img src="./media/image13.png" style="width:6.5in;height:3.63611in"
alt="A screenshot of a computer Description automatically generated" />

12. Now, click on **Data-FactoryXX** on the left-sided navigation pane.

<img src="./media/image14.png" style="width:6.5in;height:4.40833in" />

##  Task 2: Create a data pipeline

1.  Select the default Power BI icon at the bottom left of the screen,
    and switch to the **Data Factory** experience.

> <img src="./media/image15.png"
> style="width:5.64583in;height:5.94298in" />

2.  In the **Data Factory** Home page, click on **Data pipeline** as
    shown in the below image.

<img src="./media/image16.png"
style="width:6.49167in;height:5.73333in" />

3.  In the **New pipeline** dialog box, enter +++**First_Pipeline1+++**
    in the **Name** field, then click on the **Create** button.

> <img src="./media/image17.png"
> style="width:3.11667in;height:2.53333in" />

## Task 3: Use a Copy activity in the pipeline to load sample data to a data Lakehouse

1.  In the **First_Pipeline1** home page Select **Copy data assistant**
     to open the copy assistant tool.

> <img src="./media/image18.png" style="width:6.45in;height:3.575in" />

2.  The **Copy data** dialog is displayed with the first step, **Choose
    data source**, highlighted. Select  **Sample data** section, and
    select the **NYC Taxi-Green** data source type. Then
    select **Next**.

<img src="./media/image19.png"
style="width:6.97762in;height:3.17083in" />

3.  In the **Connect to data source**, click on  **Next** button.

<img src="./media/image20.png"
style="width:6.49167in;height:3.05833in" />

4.  For the **Choose data destination** step of the copy assistant,
    select **Lakehouse** and then **Next**.

<img src="./media/image21.png" style="width:7.12917in;height:3.41358in"
alt="A screenshot of a computer Description automatically generated" />

5.  Select OneLake data hub and select **Existing Lakehouse** on the
    data destination configuration page that
    appears.<img src="./media/image22.png"
    style="width:7.07796in;height:3.47083in" />

6.  Now configure the details of your Lakehouse destination on
    the **Select and map to folder path or table.** page.
    Select **Tables** for the **Root folder**, provide a table name
    +++**Bronze+++**, and select the **Next**.

> <img src="./media/image23.png" style="width:7.15139in;height:3.3875in"
> alt="A screenshot of a computer Description automatically generated" />

7.  Finally, on the **Review + save** page of the copy data assistant,
    review the configuration. For this lab, uncheck the **Start data
    transfer immediately** checkbox, since we run the activity manually
    in the next step. Then select **OK**.

<img src="./media/image24.png"
style="width:7.08748in;height:3.32083in" />

<img src="./media/image25.png" style="width:6.5in;height:3.15833in" />

## **Task 4: Run and view the results of your Copy activity**.

1.  On the **Home** tab of the pipeline editor window, then select
    the **Run** button.<img src="./media/image26.png"
    style="width:6.6375in;height:3.25067in" />

2.  In the **Save and run?** dialog box, click on **Save and run**
    button to execute these activities. This activity will take around
    11-12 min

> <img src="./media/image27.png" style="width:3.45in;height:2.26667in" />

<img src="./media/image28.png" style="width:7.14529in;height:4.54443in"
alt="A screenshot of a computer Description automatically generated" />

<img src="./media/image29.png"
style="width:7.2125in;height:4.81451in" />

3.  You can monitor the run and check the results on the **Output** tab
    below the pipeline canvas. Select the **activity name** as
    **Copy_ihy** to view the run details.

<img src="./media/image30.png" style="width:6.49167in;height:4.375in" />

4.  The run details show 76,513,115 rows read and written.

<img src="./media/image31.png" style="width:5.875in;height:7.06667in" />

5.  Expand the **Duration breakdown** section to see the duration of
    each stage of the Copy activity. After reviewing the copy details,
    select **Close**.

<img src="./media/image32.png"
style="width:5.95833in;height:7.03333in" />

**Exercise 2: Transform data with a dataflow in Data Factory**

## Task 1: Get data from a Lakehouse table

1.  On the **First_Pipeline 1** page, from the sidebar
    select **Create.**

<img src="./media/image33.png" style="width:4.55in;height:4.80833in" />

2.  On the **Data Factory Data-FactoryXX** home page, to create a new
    dataflow gen2 click on **Dataflow Gen2** under the **Data
    Factory.** 

<img src="./media/image34.png"
style="width:6.95417in;height:4.49923in" />

3.  From the new dataflow menu, under the **Power Query** pane click on
    **Get data**, then select **More...**.

> <img src="./media/image35.png"
> style="width:6.49167in;height:5.23333in" />

4.  In the **Choose data source** tab, search box search type
    +++**Lakehouse+++** and then click on the **Lakehouse** connector.

> <img src="./media/image36.png" style="width:6.49167in;height:2.775in" />

5.  The **Connect to data source** dialog appears, select **Edit
    connection.** <img src="./media/image37.png"
    style="width:6.92083in;height:3.08284in" />

6.  In the **Connect to data source** dialog box, select **sign in**
    using your Power BI organizational account to set the identity that
    the dataflow uses to access the lakehouse.

<img src="./media/image38.png"
style="width:6.92147in;height:3.07917in" />

<img src="./media/image39.png"
style="width:5.06667in;height:3.52917in" />

7.  In **Connect to data source** dialog box, select **Next.**

> <img src="./media/image40.png"
> style="width:6.91751in;height:3.04583in" />

8.  The **Choose data** dialog is displayed. Use the navigation pane to
    find the Lakehouse you created for the destination in the prior
    module, and select the **DataFactoryLakehouse** data table then
    click on **Create** button.

<img src="./media/image41.png"
style="width:7.40666in;height:3.20417in" />

9.  Once your canvas is populated with the data, you can set **column
    profile** information, as this is useful for data profiling. You can
    apply the right transformation and target the right data values
    based on it.

10. To do this, select **Options** from the ribbon pane, then select the
    first three options under **Column profile**, and then
    select **OK**.

<img src="./media/image42.png" style="width:6.5in;height:5.08333in" />

<img src="./media/image43.png" style="width:6.49167in;height:3.625in" />

## Task 2: Transform the data imported from the Lakehouse

1.  Select the data type icon in the column header of the second
    column, **IpepPickupDatetime**, to display **right click** on the
    menu and select the **Change type** from the menu to convert the
    column from the **Date/Time** to **Date** type. 

<img src="./media/image44.png"
style="width:6.49167in;height:5.41667in" />

2.  On the **Home** tab of the ribbon, select the **Choose
    columns** option from the **Manage columns** group.

<img src="./media/image45.png"
style="width:7.04583in;height:3.3375in" />

3.  On the **Choose columns** dialog, **deselect** some columns listed
    here, then select **OK**.

    - lpepDropoffDatetime

    <!-- -->

    - puLocationId

    <!-- -->

    - doLocationId

    <!-- -->

    - pickupLatitude

    <!-- -->

    - dropoffLongitude

    <!-- -->

    - rateCodeID

> <img src="./media/image46.png"
> style="width:4.26912in;height:5.4875in" />

4.  Select the **storeAndFwdFlag** column's filter and sort dropdown
    menu. (If you see a warning **List may be incomplete**,
    select **Load more** to see all the data.)

<img src="./media/image47.png"
style="width:7.34347in;height:3.4125in" />

5.  Select '**Y'** to show only rows where a discount was applied, and
    then select **OK**.

<img src="./media/image48.png"
style="width:4.92083in;height:5.33095in" />

6.  Select the **Ipep_Pickup_Datetime** column sort and filter dropdown
    menu, then select **Date filters**, and choose
    the **Between...** filter provided for Date and Date/Time types.

<img src="./media/image49.png"
style="width:6.99843in;height:4.54583in" />

11. In the **Filter rows** dialog, select dates between **January 1,
    2015**, and **January 31, 2015**, then select **OK**.

> <img src="./media/image50.png" style="width:6.425in;height:3.21667in" />

## Task 3: Connect to a CSV file containing discount data

Now, with the data from the trips in place, we want to load the data
that contains the respective discounts for each day and VendorID, and
prepare the data before combining it with the trips data.

1.  From the **Home** tab in the dataflow editor menu, select the **Get
    data** option, and then choose **Text/CSV**.

<img src="./media/image51.png" style="width:6.5in;height:5.88403in" />

2.  In the **Connect to data source** pane, under **Connection
    settings**, select **Upload file (Preview)** radio button, then
    click on **Browse** button and browse your VM **C:\LabFiles**, then
    select the **NYC-Taxi-Green-Discounts** file and click on the
    **Open** button.

<img src="./media/image52.png" style="width:6.5in;height:3.91667in" />

<img src="./media/image53.png"
style="width:6.49167in;height:4.03333in" />

3.  In the **Connect to data source** pane, click on the **Next**
    button.

<img src="./media/image54.png"
style="width:6.49167in;height:2.85833in" />

4.  On the **Preview file data** dialog, select **Create**.

<img src="./media/image55.png"
style="width:7.37173in;height:3.24583in" />

## Task 4: Transform the discount data

1.  Reviewing the data, we see the headers appear to be in the first
    row. Promote them to headers by selecting the table's context menu
    at the top left of the preview grid area to select **Use first row
    as headers**.

<img src="./media/image56.png"
style="width:6.49167in;height:5.24167in" />

*** Note:** After promoting the headers, you can see a new step added to
the **Applied steps** pane at the top of the dataflow editor to the data
types of your columns.*

2.  Right-click the **VendorID** column, and from the context menu
    displayed, select the option **Unpivot other columns**. This allows
    you to transform columns into attribute-value pairs, where columns
    become rows.

<img src="./media/image57.png" style="width:6.5in;height:5.28333in" />

3.  With the table unpivoted, rename
    the **Attribute** and **Value** columns by double-clicking them and
    changing **Attribute** to **Date** and **Value** to **Discount**.

<img src="./media/image58.png" style="width:6.5in;height:5.76667in" />

<img src="./media/image59.png" style="width:6.5in;height:5.375in" />

<img src="./media/image60.png"
style="width:6.76893in;height:4.92917in" />

<img src="./media/image61.png" style="width:6.5in;height:5.80833in" />

4.  Change the data type of the Date column by selecting the data type
    menu to the left of the column name and choosing **Date**.

<img src="./media/image62.png" style="width:6.49167in;height:4.825in" />

5.  Select the **Discount** column and then select the **Transform** tab
    on the menu. Select **Number column**, and then
    select **Standard** numeric transformations from the submenu, and
    choose **Divide**.

<img src="./media/image63.png"
style="width:7.35248in;height:3.87917in" />

6.  On the **Divide** dialog, enter the value +++100+++, then click on
    **OK** button.

<img src="./media/image64.png" style="width:4.21667in;height:2.175in" />

**Task 5: Combine trips and discounts data**

The next step is to combine both tables into a single table that has the
discount that should be applied to the trip, and the adjusted total.

1.  First, toggle the **Diagram view** button so you can see both of
    your queries.

<img src="./media/image65.png"
style="width:7.24888in;height:3.82917in" />

2.  Select the **Bronze** query, and on the **Home** tab, Select
    the **Combine** menu and choose **Merge queries**, then **Merge
    queries as new**.

<img src="./media/image66.png" style="width:7.3271in;height:3.1375in" />

3.  On the **Merge** dialog,
    select **Generated-NYC-Taxi-Green-Discounts** from the **Right table
    for merge** drop down, and then select the "**light bulb**" icon on
    the top right of the dialog to see the suggested mapping of columns
    between the three tables.

<img src="./media/image67.png"
style="width:7.00069in;height:4.4125in" />

4.  Choose each of the two suggested column mappings, one at a time,
    mapping the VendorID and date columns from both tables. When both
    mappings are added, the matched column headers are highlighted in
    each table.

<img src="./media/image68.png"
style="width:7.12745in;height:4.4375in" />

5.  A message is shown asking you to allow combining data from multiple
    data sources to view the results. Select **OK** 

<img src="./media/image69.png" style="width:6.05833in;height:5.925in" />

6.  In the table area, you'll initially see a warning that "The
    evaluation was canceled because combining data from multiple sources
    may reveal data from one source to another. Select continue if the
    possibility of revealing data is okay." Select **Continue** to
    display the combined data.

<img src="./media/image70.png"
style="width:7.38116in;height:2.6875in" />

7.  In Privacy Levels dialog box, select the **check box :Ignore Privacy
    Lavels checks for this document. Ignoring privacy Levels could
    expose sensitive or confidential data to an unauthorized person**
    and click on the **Save** button.

<img src="./media/image71.png"
style="width:7.28629in;height:2.89583in" />

<img src="./media/image72.png" style="width:7.34053in;height:3.56361in"
alt="A screenshot of a computer Description automatically generated" />

8.  Notice how a new query was created in Diagram view showing the
    relationship of the new Merge query with the two queries you
    previously created. Looking at the table pane of the editor, scroll
    to the right of the Merge query column list to see a new column with
    table values is present. This is the "Generated NYC
    Taxi-Green-Discounts" column, and its type is **\[Table\]**. In the
    column header there's an icon with two arrows going in opposite
    directions, allowing you to select columns from the table. Deselect
    all of the columns except **Discount**, and then select **OK**.

<img src="./media/image73.png"
style="width:7.32083in;height:2.81932in" />

<img src="./media/image74.png" style="width:7.37338in;height:3.46848in"
alt="A screenshot of a computer Description automatically generated" />

9.  With the discount value now at the row level, we can create a new
    column to calculate the total amount after discount. To do so,
    select the **Add column** tab at the top of the editor, and
    choose **Custom column** from the **General** group.

<img src="./media/image75.png" style="width:6.49167in;height:4.45in" />

10. On the **Custom column** dialog, you can use the [Power Query
    formula language (also known as
    M)](https://learn.microsoft.com/en-us/powerquery-m) to define how
    your new column should be calculated.
    Enter +++**TotalAfterDiscount+++** for the **New column name**,
    select **Currency** for the **Data type**, and provide the following
    M expression for the **Custom column formula**:

> *<span class="mark">+++if \[totalAmount\] \> 0 then \[totalAmount\] \*
> ( 1 -\[Discount\] ) else \[totalAmount\]</span>+++*
>
> Then select **OK**.

<img src="./media/image76.png"
style="width:6.49167in;height:4.48333in" />

<img src="./media/image77.png" style="width:7.36164in;height:3.61318in"
alt="A screenshot of a computer Description automatically generated" />

11. Select the newly create **TotalAfterDiscount** column and then
    select the **Transform** tab at the top of the editor window. On
    the **Number column** group, select the **Rounding** drop down and
    then choose **Round...**.

<img src="./media/image78.png"
style="width:7.05679in;height:3.2125in" />

12. On the **Round** dialog, enter **2** for the number of decimal
    places and then select **OK**.

> <img src="./media/image79.png" style="width:3.41667in;height:2in" />

13. Change the data type of the **IpepPickupDatetime** from **Date** to
    **Date/Time**.

<img src="./media/image80.png"
style="width:6.49167in;height:6.03333in" />

<img src="./media/image81.png" style="width:7.12862in;height:3.89942in"
alt="A screenshot of a computer Description automatically generated" />

14. Finally, expand the **Query settings** pane from the right side of
    the editor if it isn't already expanded, and rename the query
    from **Merge** to **Output**.

<img src="./media/image82.png"
style="width:7.03064in;height:2.44583in" />

<img src="./media/image83.png"
style="width:5.40417in;height:3.8517in" />

**Task 6: Load the output query to a table in the Lakehouse**

With the output query now fully prepared and with data ready to output,
we can define the output destination for the query.

1.  Select the **Output** merge query created previously. Then select
    the **Home** tab in the editor, and **Add data destination** from
    the **Query** grouping, to select a **Lakehouse** destination.

<img src="./media/image84.png"
style="width:7.17917in;height:3.47439in" />

2.  On the **Connect to data destination** dialog, your connection
    should already be selected. Select **Next** to continue.

<img src="./media/image85.png" style="width:6.5in;height:2.78333in" />

3.  On the **Choose destination target** dialog, browse to the Lakehouse
    where you wish to load the data and name the new
    table+++ **nyc_taxi_with_discounts+++**, then select **Next** again.

<img src="./media/image86.png"
style="width:7.36706in;height:3.12083in" />

4.  On the **Choose destination settings** dialog, leave the
    default **Replace** update method, double check that your columns
    are mapped correctly, and select **Save settings**.

<img src="./media/image87.png"
style="width:7.09165in;height:3.00417in" />

5.  Back in the main editor window, confirm that you see your output
    destination on the **Query settings** pane for the **Output** table,
    and then select **Publish**.

<img src="./media/image88.png"
style="width:7.2572in;height:3.55417in" />

<img src="./media/image89.png" style="width:7.27868in;height:3.77698in"
alt="A screenshot of a computer Description automatically generated" />

6.  On the workspace page, you can rename your dataflow by selecting the
    ellipsis to the right of the dataflow name that appears after you
    select the row, and choosing **Properties**.

<img src="./media/image90.png" style="width:6.5in;height:4.91667in" />

7.  In the **Dataflow 1** dialog box,
    enter +++**nyc_taxi_data_with_discounts+++** in the name box, then
    select **Save**.

> <img src="./media/image91.png" style="width:6.5in;height:6.88333in" />

8.  Select the refresh icon for the dataflow after selecting its row,
    and when complete, you should see your new Lakehouse table created
    as configured in the **Data destination** settings.

<img src="./media/image92.png"
style="width:6.49167in;height:3.74167in" />

9.  In the **Data_FactoryXX** pane, select **DataFactoryLakehouse** to
    view the new table loaded there.

<img src="./media/image93.png" style="width:6.5in;height:3.29167in" />

<img src="./media/image94.png" style="width:6.5in;height:3.81667in" />

# Exercise 3: Automate and send notifications with Data Factory

** Important**

Microsoft Fabric is currently in PREVIEW. This information relates to a
prerelease product that may be substantially modified before it's
released. Microsoft makes no warranties, expressed or implied, with
respect to the information provided here. Refer to [**<u>Azure Data
Factory
documentation</u>**](https://learn.microsoft.com/en-us/azure/data-factory/) for
the service in Azure.

## Task 1: Add an Office 365 Outlook activity to your pipeline

1.  From **Tutorial_Lakehouse** page, navigate and click on
    **Data_FactoryXX** Workspace on the left-sided navigation menu.

<img src="./media/image95.png"
style="width:5.22917in;height:4.72361in" />

2.  In the **Data_FactoryXX** view, select the **First_Pipeline1**.

<img src="./media/image96.png"
style="width:7.27917in;height:4.37683in" />

3.  Select the **Activities** tab in the pipeline editor and find the
    **Office Outlook** activity.

<img src="./media/image97.png"
style="width:7.22709in;height:2.14583in" />

4.  Select and drag the **On success** path (a green checkbox on the top
    right side of the activity in the pipeline canvas) from your **Copy
    activity** to your new **Office 365 Outlook** activity.

<img src="./media/image98.png" style="width:7.322in;height:2.82917in" />

5.  Select the Office 365 Outlook activity from the pipeline canvas,
    then select the **Settings** tab of the property area below the
    canvas to configure the email. Click on **Sing in** button.

<img src="./media/image99.png"
style="width:7.34111in;height:4.42917in" />

6.  Select your Power BI organizational account and then select **Allow
    access** to confirm.

<img src="./media/image100.png"
style="width:5.03333in;height:3.72083in" />

<img src="./media/image101.png"
style="width:5.09167in;height:4.3125in" />

**Note:** The service doesn't currently support personal email. You must
use an enterprise email address.

7.  Select the Office 365 Outlook activity from the pipeline canvas, on
    the **Settings** tab of the property area below the canvas to
    configure the email.

    - Enter your email address in the **To** section. If you want to use
      several addresses, use **;** to separate them.

    <!-- -->

    - For the **Subject**, select the field so that the **Add dynamic
      content** option appears, and then select it to display the
      pipeline expression builder canvas.

<img src="./media/image102.png"
style="width:7.15878in;height:4.75417in" />

8.  The **Pipeline expression builder** dialog appears. Enter the
    following expression, then select **OK**:

> *<span class="mark">@concat('DI in an Hour Pipeline Succeeded with
> Pipeline Run Id', pipeline().RunId)</span>*

<img src="./media/image103.png"
style="width:6.49167in;height:6.46667in" />

9.  For the **Body**, select the field again and choose the **View in
    expression builder** option when it appears below the text area. Add
    the following expression again in the **Pipeline expression
    builder** dialog that appears, then select **OK**:

> *<span class="mark">@concat('RunID = ', pipeline().RunId, ' ; ',
> 'Copied rows ', activity('Copy data1').output.rowsCopied, ' ;
> ','Throughput ', activity('Copy data1').output.throughput)</span>*

<img src="./media/image104.png" style="width:6.5in;height:4.3in" />

<img src="./media/image105.png"
style="width:6.6375in;height:6.52688in" />

** Note:** Replace **Copy data1** with the name of your own pipeline
copy activity.

10. Finally select the **Home** tab at the top of the pipeline editor,
    and choose **Run**. Then select **Save and run** again on the
    confirmation dialog to execute these activities.

> <img src="./media/image106.png"
> style="width:6.95947in;height:4.02917in" />
>
> <img src="./media/image107.png"
> style="width:3.78333in;height:2.51667in" />
>
> <img src="./media/image108.png" style="width:6.5in;height:3.33958in" />

11. After the pipeline runs successfully, check your email to find the
    confirmation email sent from the pipeline.

<img src="./media/image109.png" style="width:6.72938in;height:3.72129in"
alt="A screenshot of a computer Description automatically generated" />

<img src="./media/image110.png"
style="width:6.78215in;height:2.2375in" />

**Task 2: Schedule pipeline execution**

Once you finish developing and testing your pipeline, you can schedule
it to execute automatically.

1.  On the **Home** tab of the pipeline editor window,
    select **Schedule**.

<img src="./media/image111.png"
style="width:6.49167in;height:3.73333in" />

2.  Configure the schedule as required. The example here schedules the
    pipeline to execute daily at 8:00 PM until the end of the year.

<img src="./media/image112.png"
style="width:6.9625in;height:5.45396in" />

***Task 3:* Add a Dataflow activity to the pipeline**

1.  Hover over the green line connecting the **Copy activity** and the
    **Office 365 Outlook** activity on your pipeline canvas, and select
    the **+** button to insert a new activity.

> <img src="./media/image113.png" style="width:6.5in;height:3.16667in" />

2.  Choose **Dataflow** from the menu that appears.

<img src="./media/image114.png" style="width:6.5in;height:3.025in" />

3.  The newly created Dataflow activity is inserted between the Copy
    activity and the Office 365 Outlook activity, and selected
    automatically, showing its properties in the area below the canvas.
    Select the **Settings** tab on the properties area, and then select
    your dataflow created in **Exercise 2: Transform data with a
    dataflow in Data Factory**.

<img src="./media/image115.png"
style="width:7.02691in;height:2.9625in" />

12. Select the **Home** tab at the top of the pipeline editor, and
    choose **Run**. Then select **Save and run** again on the
    confirmation dialog to execute these activities.

<img src="./media/image116.png"
style="width:6.49167in;height:2.375in" />

<img src="./media/image117.png" style="width:3.73333in;height:2.35in" />

<img src="./media/image118.png" style="width:7.25253in;height:3.55885in"
alt="A screenshot of a computer Description automatically generated" />

## Task 4: Clean up resources

You can delete individual reports, pipelines, warehouses, and other
items or remove the entire workspace. Use the following steps to delete
the workspace you created for this tutorial.

1.  Select your workspace, the **Data-FactoryXX** from the left-hand
    navigation menu. It opens the workspace item view.

<img src="./media/image119.png"
style="width:4.81667in;height:5.83333in" />

2.  Select the ***...*** option under the workspace name and
    select **Workspace settings**.

<img src="./media/image120.png"
style="width:6.49167in;height:4.075in" />

3.  Select **Other** and **Remove this workspace.**

<img src="./media/image121.png" style="width:6.5in;height:3.9in" />
