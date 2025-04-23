用例 03：用于使用数据流和数据管道移动和转换数据的数据工厂解决方案

**介绍**

此实验室通过在一小时内为完整数据集成方案提供分步指导，帮助你加快
Microsoft Fabric
中数据工厂的评估过程。在本教程结束时，你将了解数据工厂的价值和关键功能，并知道如何完成常见的端到端数据集成方案。

**目的**

该实验室分为三个模块:

- 练习 1：使用数据工厂创建管道 ，以将原始数据从 Blob
  存储引入数据湖仓一体中的 Bronze 表。

- 练习 2：使用数据工厂中的数据流转换数据，以处理 Bronze
  表中的原始数据，并将其移动到数据 Lakehouse 中的 Gold 表。

- 练习
  3：使用数据工厂自动执行并发送通知，以便在所有作业完成后发送电子邮件通知您，最后，将整个流程设置为按计划运行。

# 练习 1：使用数据工厂创建管道

** 重要**

Microsoft Fabric
目前为预览版。此信息与预发行产品相关，该产品在发布之前可能会进行重大修改。Microsoft
对此处提供的信息不作任何明示或暗示的保证。请参阅 [***Azure Data Factory
documentation***](https://learn.microsoft.com/en-us/azure/data-factory/)，了解
Azure 中的服务。

## 任务 1：创建工作区

在 Fabric 中处理数据之前，请创建一个启用了 Fabric 试用版的工作区。

1.  打开浏览器，导航到地址栏，然后键入或粘贴以下
    URL：<https://app.fabric.microsoft.com/> 然后按 **Enter** 按钮。

> ![A screenshot of a computer Description automatically
> generated](./media/image1.png)
>
> **注意**：如果您被定向到 Microsoft Fabric 主页，请跳过从 \#2 到 \#4
> 的步骤。

2.  在 **Microsoft Fabric** 窗口中，输入您的凭据，然后单击 **Submit**
    按钮。

> ![A close up of a white and green object Description automatically
> generated](./media/image2.png)

3.  然后，在 **Microsoft** 窗口中，输入密码并单击 **Sign in** 按钮**。**

> ![A login screen with a red box and blue text Description
> automatically generated](./media/image3.png)

4.  在 **Stay signed in?** 窗口中，单击 **Yes** 按钮。

> ![A screenshot of a computer error Description automatically
> generated](./media/image4.png)

5.  点击导航栏中的 **+New workshop** 按钮创建新的 Eventhouse。

> ![A screenshot of a computer AI-generated content may be
> incorrect.](./media/image5.png)

6.  在 **Create a workspace** 选项卡中，输入以下详细信息，然后单击
    **Apply** 按钮 。

[TABLE]

> ![](./media/image6.png)
>
> ![A screenshot of a computer Description automatically
> generated](./media/image7.png)
>
> ![](./media/image8.png)

7.  等待部署完成。大约需要 2-3 分钟。

8.  在 **Data-FactoryXX** 工作区页面中，导航并单击 **+New item**
    按钮，然后选择 **Lakehouse**。

![A screenshot of a browser AI-generated content may be
incorrect.](./media/image9.png)

> ![A screenshot of a computer AI-generated content may be
> incorrect.](./media/image10.png)

9.  在 **New lakehouse** 对话框中，在 **Nam**e 字段中输入
    +++**DataFactoryLakehouse**+++，单击 **Create** 按钮并打开新的
    Lakehouse。

![A screenshot of a computer AI-generated content may be
incorrect.](./media/image11.png)

![A screenshot of a computer Description automatically
generated](./media/image12.png)

10. 现在，单击左侧导航窗格中的 **Data-FactoryXX**。

![](./media/image13.png)

##  任务 2：创建数据管道

1.  通过单击导航栏中的 +New item 按钮创建新的湖仓一体。单击“Data
    pipeline”磁![A screenshot of a computer AI-generated content may be
    incorrect.](./media/image14.png)。

2.  在 **New pipeline** 对话框中，在 **Name** 字段中输入
    +++**First_Pipeline1**+++，然后单击 **Create** 按钮。

![](./media/image15.png)

## 任务 3：使用管道中的 Copy 活动将示例数据加载到数据 Lakehouse

1.  在 **First_Pipeline1** 主页 中，选择 **Copy data assistant**
    以打开复制助手工具。

> ![](./media/image16.png)

2.  此时将显示 **Copy data** 对话框，其中突出显示了第一步 **Choose data
    source**。选择 **Sample data** section，然后选择 **NYC Taxi-Green**
    数据源类型。然后选择 **Next**。

![](./media/image17.png)

3.  在 **Connect to data source** 中，单击 **Next** 按钮。

![](./media/image18.png)

4.  对于复制助手的 **Choose data destination** 步骤，选择
    **Lakehouse**，然后选择 **Next**。

![A screenshot of a computer Description automatically
generated](./media/image19.png)

5.  选择 OneLake Data Hub，然后在 出现的数据目标配置页面上选择
    **Existing Lakehouse**。![](./media/image20.png)

6.  现在，在 **Select and map to folder path or table** 上配置 Lakehouse
    目标的详细信息**。** 页。为 **Root folder** 选择
    **Tables**，提供表名称 +++**Bronze**+++，然后选择 **Next**。

> ![A screenshot of a computer Description automatically
> generated](./media/image21.png)

7.  最后，在复制数据助手的 **Review + save**
    页面上，查看配置。对于此实验，请取消选中 **Start data transfer
    immediately** 复选框，因为我们在下一步中手动运行活动。然后选择
    **OK。**

![](./media/image22.png)

![](./media/image23.png)

## **任务 4：运行并查看 Copy 活动的结果**。

1.  在 管道编辑器窗口的 **Home** 选项卡上，选择 **Run**
    按钮。![](./media/image24.png)

2.  在 **Save and run？** 对话框中， 单击 **Save and run**
    按钮以执行这些活动。此活动大约需要 11-12 分钟

> ![](./media/image25.png)

![A screenshot of a computer Description automatically
generated](./media/image26.png)

![](./media/image27.png)

3.  您可以在管道画布下方的 Output 选项卡上监控运行并检查结果 。选择
    **activity name** 作为 **Copy_ihy** 以查看运行详细信息。

![](./media/image28.png)

4.  运行详细信息显示读取和写入的 76,513,115 行。

![](./media/image29.png)

5.  展开 **Duration breakdown** 部分，以查看 Copy
    活动的每个阶段的持续时间。查看复制详细信息后，选择 **Close**。

![](./media/image30.png)

**练习 2：使用数据工厂中的数据流转换数据**

## 任务 1：从 Lakehouse 表中获取数据

1.  在 **First_Pipeline 1** 页面上，从侧边栏中选择 **Create。**

![](./media/image31.png)

2.  在 **Data Factory Data-FactoryXX** 主页上，要创建新的数据流第 2
    代，请单击 **Data Factory** 下的 **Dataflow Gen2。**

![](./media/image32.png)

3.  从新的数据流菜单中，在 **Power Query** 窗格下，单击 **Get
    data**，然后选择 **More...**。

![](./media/image33.png)

> ![](./media/image34.png)

4.  在 **Choose data source** 选项卡中，搜索框搜索键入
    +++**Lakehouse**+++，然后单击 **Lakehouse** 连接器。

> ![](./media/image35.png)

5.  此时将显示 **Connect to data source** 对话框，选择 **Edit
    connection.** ![](./media/image36.png)

6.  在 **Connect to data source** 对话框中，选择 **Sign in** using your
    Power BI 组织帐户 以设置数据流用于访问湖仓一体的身份。

![](./media/image37.png)

![](./media/image38.png)

7.  在 **Connect to data source** 对话框中，选择 **Next。**

![](./media/image39.png)

> ![](./media/image40.png)

8.  此时将显示 **Choose data**
    对话框。使用导航窗格查找您在上一个模块中为目标创建的
    Lakehouse，然后选择 **DataFactoryLakehouse** 数据表，然后单击
    **Create** 按钮。

![](./media/image41.png)

9.  在画布中填充数据后，您可以设置 **column profile**
    信息，因为这对于数据概要分析非常有用。您可以应用正确的转换，并基于它定位正确的数据值。

10. 为此，请从功能区窗格中选择 **Options** ，然后在 Column
    profile下选择前三个选项，然后选择 **OK**。

![](./media/image42.png)

![](./media/image43.png)

## 任务 2：转换从 Lakehouse 导入的数据

1.  选择第二列列标题中的数据类型图标 **IpepPickupDatetime**
    以显示**右键单击**菜单，然后从菜单中选择 **Change type** 以将列从
    **Date/Time** 转换为 **Date** 类型。

![](./media/image44.png)

2.  在 功能区的 **Home** 选项卡上，从 **Choose columns** 组中选择
    **Manage columns** 选项。

![](./media/image45.png)

3.  在 **Choose columns** 对话框中，**deselect**
    选择此处列出的一些列，然后选择 **OK**。

    - lpepDropoffDatetime

    &nbsp;

    - puLocationId

    &nbsp;

    - doLocationId

    &nbsp;

    - pickupLatitude

    &nbsp;

    - dropoffLongitude

    &nbsp;

    - rateCodeID

> ![](./media/image46.png)

4.  选择 **storeAndFwdFlag** 列的 **filter and sort**
    下拉菜单。（如果您看到警告 列表可能不完整，请选择 **Load more**
    以查看所有数据。

![](./media/image47.png)

5.  选择“**Y”**以仅显示应用了折扣的行，然后选择 **OK。**

![](./media/image48.png)

6.  选择 **Ipep_Pickup_Datetime** 列排序和筛选器下拉菜单，然后选择
    日期筛选器，然后选择 **Between...** filter 为 Date 和 Date/Time
    类型提供。

![](./media/image49.png)

11. 在 **Filter rows** 对话框中，选择 **2015 年 1 月 1** 日至 **2015 年
    1 月 31 日**之间的日期，然后选择 **OK**。

> ![](./media/image50.png)

## 任务 3：连接到包含折扣数据的 CSV 文件

现在，有了行程的数据，我们想要加载包含每天的相应折扣和 VendorID
的数据，并在将数据与行程数据合并之前准备数据。

1.  从 **Home** 选项卡的数据流编辑器菜单中，选择 **Get data **
    选项，然后选择 **Text/CSV。**

![](./media/image51.png)

2.  在**Connect to data source** 窗格的 **Connection settings 下**，选择
    **Upload file (Preview)** 单选按钮，然后单击
    **Browse**按钮并浏览您的 VM **C：\LabFiles**，然后选择
    **NYC-Taxi-Green-Discounts** 文件并单击 **Open** 按钮。

![](./media/image52.png)

![](./media/image53.png)

3.  在 **Connect to data source** 窗格中，单击 **Next** 按钮。

![](./media/image54.png)

4.  在 **Preview file data** 对话框中，选择 **Create**。

![](./media/image55.png)

## 任务 4：转换折扣数据

1.  查看数据，我们看到标题似乎位于第一行。通过选择预览网格区域左上角的表上下文菜单以选择
    **Use first row as headers ，**将它们提升到标题。

![](./media/image56.png)

***注意：**提升标题后，您可以看到一个新步骤添加到数据流编辑器顶部的
**Applied steps** 窗格中，该步骤添加到列的数据类型中。*

2.  右键单击 **VendorID** 列，然后从显示的上下文菜单中选择选项 **Unpivot
    other columns**。这允许您将列转换为属性-值对，其中列变为行。

![](./media/image57.png)

3.  在表未透视的情况下，双击 **Attribute** 和 **Value** 列，然后将
    **Attribute** 更改为 **Date**，将 **Value** 更改为
    **Discount**，从而重命名这些列。

![](./media/image58.png)

![](./media/image59.png)

![](./media/image60.png)

![](./media/image61.png)

4.  通过选择列名称左侧的数据类型菜单并选择 **Date** 来更改 Date
    列的数据类型。

![](./media/image62.png)

5.  选择 **Discount** 列，然后选择 菜单上的 **Transform** 选项卡。选择
    **Number column**，然后从 子菜单中选择 **Standard** numeric
    transformations，然后选择 **Divide**。

![](./media/image63.png)

6.  在 ** Divide** 对话框中，输入值 +++100+++，然后单击 **OK** 按钮。

![](./media/image64.png)

**任务 5：合并行程和折扣数据**

下一步是将两个表合并到一个表中，该表包含应应用于行程的折扣和调整后的总计。

1.  首先，切换 **Diagram view** 按钮，以便您可以看到两个查询。

![](./media/image65.png)

2.  选择 **Bronze** 查询，然后在 **Home** 选项卡上，选择 **Combine**
    菜单，然后选择 **Merge queries**，然后选择 **Merge queries as
    new**。

![](./media/image66.png)

3.  在 **Merge** 对话框中， **从** Right table for merge
    **下拉列表中选择
    Generated-NYC-Taxi-Green-Discounts**，然后选择对话框右上角的
    "**light bulb"** 图标，以查看三个表之间建议的列映射。

![](./media/image67.png)

4.  选择两个建议的列映射中的每一个，一次一个，映射两个表中的 VendorID 和
    date 列。添加两个映射后，匹配的列标题将在每个表中突出显示。

![](./media/image68.png)

5.  此时将显示一条消息，要求您允许合并来自多个数据源的数据以查看结果。选择
    **OK**

![](./media/image69.png)

6.  在表区域中，您最初会看到一条警告，指出“评估已取消，因为合并来自多个源的数据可能会将数据从一个源显示到另一个源。如果可以显示数据，请选择
    continue （继续）。选择 **Continue** 以显示组合数据。

![](./media/image70.png)

7.  在 Privacy Levels 对话框中，选中 **check box :Ignore Privacy Lavels
    checks for this document. Ignoring privacy Levels could expose
    sensitive or confidential data to an unauthorized person**，然后单击
    **Save** 按钮。

![](./media/image71.png)

![A screenshot of a computer Description automatically
generated](./media/image72.png)

8.  请注意在关系图视图中创建新查询的方式，其中显示了新 Merge
    查询与之前创建的两个查询的关系。查看编辑器的表窗格，滚动到 Merge
    query column （合并查询列）
    列表的右侧，以查看存在具有表值的新列。这是“Generated NYC
    Taxi-Green-Discounts”列，其类型为
    **\[Table\]。**在列标题中，有一个图标，其中有两个相反方向的箭头，允许您从表中选择列。取消选择除
    **Discount** 之外的所有列，然后选择 **OK。**

![](./media/image73.png)

![A screenshot of a computer Description automatically
generated](./media/image74.png)

9.  现在，discount 值位于行级别，我们可以创建一个新列来计算 discount
    后的总金额。为此，请选择编辑器顶部的 **Add column** 选项卡，然后从
    **General** 组中选择 **Custom column**。

![](./media/image75.png)

10. 在 **Custom column** 对话框中，您可以使用 [Power Query 公式语言
    （也称为 M）](https://learn.microsoft.com/en-us/powerquery-m)
    来定义新列的计算方式。输入 +++**TotalAfterDiscount**+++ 作为 **New
    column name**，选择 **Currency** 作为 **Data type**，并为 **Custom
    column** formula 提供以下 M 表达式：

> *+++if \[totalAmount\] \> 0 then \[totalAmount\] \* ( 1 -\[Discount\]
> ) else \[totalAmount\]+++*
>
> 然后选择 **OK。**

![](./media/image76.png)

![A screenshot of a computer Description automatically
generated](./media/image77.png)

11. 选择新创建的 **TotalAfterDiscount** 列，然后选择 编辑器窗口顶部的
    Transform 选项卡。在 **Number column** group 组中，选择 **Rounding**
    下拉列表，然后选择 **Round...**。

![](./media/image78.png)

12. 在 **Round** 对话框中，输入 **2** 作为小数位数，然后选择 **OK。**

> ![](./media/image79.png)

13. 将 **IpepPickupDatetime** 的数据类型 从 **Date** 更改为
    **Date/Time**。

![](./media/image80.png)

![A screenshot of a computer Description automatically
generated](./media/image81.png)

14. 最后，从编辑器右侧展开 **Query settings**
    窗格（如果尚未展开），然后将查询从 **Merge** 重命名为 **Output**。

![](./media/image82.png)

![](./media/image83.png)

**任务 6：将输出查询加载到 Lakehouse 中的表**

输出查询现已完全准备好，数据已准备好输出，我们可以定义查询的输出目标。

1.  选择 **Output** merge query created 之前创建。然后选择编辑器中的
    **Home** 选项卡，并从 **Query 分**组中选择 **Add data destination**
    以选择 **Lakehouse** 目标。

![](./media/image84.png)

2.  在 **Connect to data destination** 对话框中，您的连接应已选中。选择
    **Next** 继续。

![](./media/image85.png)

3.  在 **Choose destination target** 对话框中，浏览到要加载数据的
    Lakehouse，并将新表命名为 +++
    **nyc_taxi_with_discounts+++**，然后再次选择 **Next**。

![](./media/image86.png)

4.  在 **Choose destination settings** 对话框中，保留默认的 **Replace**
    update method，仔细检查您的列是否正确映射，然后选择 **Save
    settings**。

![](./media/image87.png)

5.  返回主编辑器窗口，确认您在 **Output** 表的 **Query settings**
    窗格中看到输出目标，然后选择 **Publish**。

![](./media/image88.png)

![A screenshot of a computer Description automatically
generated](./media/image89.png)

6.  在工作区页面上，您可以通过选择选择行后显示的数据流名称右侧的省略号，然后选择
    **Properties** 来重命名数据流。

![](./media/image90.png)

7.  在 **Dataflow 1** 对话框中，在名称框中输入
    +++**nyc_taxi_data_with_discounts**+++，然后选择 **Save**。

> ![](./media/image91.png)

8.  选择数据流的行后，选择数据流的刷新图标，完成后，您应该会看到按照
    **Data destination** 设置中的配置创建了新的 Lakehouse 表 。

![](./media/image92.png)

9.  在 **Data_FactoryXX** 窗格中， 选择 **DataFactoryLakehouse**
    以查看其中加载的新表。

![](./media/image93.png)

![](./media/image94.png)

# 练习 3：使用数据工厂自动执行和发送通知

**重要**

Microsoft Fabric
目前为预览版。此信息与预发行产品相关，该产品在发布之前可能会进行重大修改。Microsoft
对此处提供的信息不作任何明示或暗示的保证。请参阅 [***Azure Data Factory
documentation***](https://learn.microsoft.com/en-us/azure/data-factory/)，了解
Azure 中的服务。

## 任务 1：将 Office 365 Outlook 活动添加到管道

1.  在 **Tutorial_Lakehouse** 页面中，导航并单击 左侧导航菜单上的
    **Data_FactoryXX** Workspace。

![](./media/image95.png)

2.  在 **Data_FactoryXX** 视图中，选择 **First_Pipeline1**。

![](./media/image96.png)

3.  在管道编辑器中选择 **Activities** 选项卡，然后找到 **Office
    Outlook** 活动。

![](./media/image97.png)

4.  选择 **On success** 路径（管道画布中活动右上角的绿色复选框）并将其从
    **Copy activity** 拖动到新的 **Office 365 Outlook** 活动。

![](./media/image98.png)

5.  从管道画布中选择 Office 365 Outlook 活动，然后选择
    画布下方属性区域的 **Settings** 选项卡以配置电子邮件。点击 **Sing
    in** 按钮。

![](./media/image99.png)

6.  选择您的 Power BI 组织帐户， 然后选择 **Allow access** 进行确认。

![](./media/image100.png)

![](./media/image101.png)

**注意：** 该服务目前不支持个人电子邮件。您必须使用企业电子邮件地址。

7.  从管道画布中选择 Office 365 Outlook 活动，在 画布下方属性区域的
    **Settings** 选项卡上，以配置电子邮件。

    - 在 **To** 部分。如果要使用多个地址，请使用 ;将它们分开。

    &nbsp;

    - 对于 **Subject**，选择字段以显示 **Add dynamic content**
      选项，然后选择它以显示管道表达式生成器画布。

![](./media/image102.png)

8.  此时将显示 **Pipeline expression builder**
    对话框。输入以下表达式，然后选择 **OK：**

> *@concat('DI in an Hour Pipeline Succeeded with Pipeline Run Id',
> pipeline().RunId)*

![](./media/image103.png)

9.  对于 **Body**，再次选择该字段，然后在文本区域下方显示 **View in
    expression builder** 选项时选择该选项。在出现的 **Pipeline
    expression builder** 对话框中再次添加以下表达式，然后选择 **OK：**

> *@concat('RunID = ', pipeline().RunId, ' ; ', 'Copied rows ',
> activity('Copy data1').output.rowsCopied, ' ; ','Throughput ',
> activity('Copy data1').output.throughput)*

![](./media/image104.png)

![](./media/image105.png)

** 注意：** 将 **Copy data1** 替换为您自己的管道复制活动的名称。

10. 最后，选择 管道编辑器顶部的 **Home** 选项卡，然后选择 Run。然后选择
    **Save and run** 再次在确认对话框中执行这些活动。

> ![](./media/image106.png)
>
> ![](./media/image107.png)
>
> ![](./media/image108.png)

11. 管道成功运行后，检查您的电子邮件以查找从管道发送的确认电子邮件。

![A screenshot of a computer Description automatically
generated](./media/image109.png)

![](./media/image110.png)

**任务 2：计划管道执行**

完成管道的开发和测试后，您可以安排它自动执行。

1.  在 管道编辑器窗口的 **Home** 选项卡上，选择 **Schedule**。

![](./media/image111.png)

2.  根据需要配置计划。此处的示例将管道安排为每天晚上 8：00
    执行，直到年底。

![](./media/image112.png)

***任务 3：* 将 Dataflow 活动添加到管道**

1.  将鼠标悬停在管道画布上连接 **Copy activity** 和 **Office 365
    Outlook** 活动的绿线上，然后选择 **+** 按钮以插入新活动。

> ![](./media/image113.png)

2.  从 显示的菜单中选择 **Dataflow**。

![](./media/image114.png)

3.  新创建的 Dataflow 活动将插入到 Copy 活动和 Office 365 Outlook
    活动之间，并自动选中，并在画布下方的区域中显示其属性。选择
    属性区域上的 **Settings** 选项卡，然后选择在 **Exercise 2: Transform
    data with a dataflow in Data Factory**。

![](./media/image115.png)

12. 选择 管道编辑器顶部的 **Home** 选项卡，然后选择 **Run**。然后选择
    **Save and run** 再次在确认对话框中执行这些活动。

![](./media/image116.png)

![](./media/image117.png)

![A screenshot of a computer Description automatically
generated](./media/image118.png)

## 任务 4：清理资源

您可以删除单个报表、管道、仓库和其他项目，也可以删除整个工作区。使用以下步骤删除您为本教程创建的工作区。

1.  从左侧导航菜单中选择您的工作区
    **Data-FactoryXX**。此时将打开工作区项视图。

![](./media/image119.png)

2.  选择 ***...*** 选项，然后选择 **Workspace settings**。

![](./media/image120.png)

3.  选择 **Other** 和 **Remove this workspace。**

![](./media/image121.png)
