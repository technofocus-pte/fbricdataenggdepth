# 사용 사례 04: Azure Databricks 및 Microsoft Fabric을 사용한 최신 클라우드 규모 분석

**소개**

이 랩에서는 Azure Databricks와 Microsoft Fabric의 통합을 통해 Medallion
아키텍처를 사용하여 Lakehouse를 만들고 관리하고, Azure Databricks를
사용하여 ADLS(Azure Data Lake Storage) Gen2 계정의 도움으로 델타
테이블을 만들고, Azure Databricks를 사용하여 데이터를 수집하는 방법을
살펴봅니다. 이 실습 가이드는 Lakehouse를 만들고, Lakehouse에 데이터를
로드하고, 구조화된 데이터 계층을 탐색하여 효율적인 데이터 분석 및 보고를
용이하게 하는 데 필요한 단계를 안내합니다.

메달리온 아키텍처는 세 개의 고유한 계층(또는 영역)으로 구성됩니다.

- 브론즈: 원시 영역이라고도 하는 이 첫 번째 계층은 원본 데이터를 원래
  형식으로 저장합니다. 이 계층의 데이터는 일반적으로 추가 전용이며
  변경할 수 없습니다.

- 실버: 보강된 영역이라고도 하는 이 레이어는 브론즈 레이어에서 가져온
  데이터를 저장합니다. 원시 데이터는 정리되고 표준화되었으며 이제
  테이블(행 및 열)로 구조화되었습니다. 또한 다른 데이터와 통합하여 고객,
  제품 등과 같은 모든 비즈니스 엔터티에 대한 엔터프라이즈 보기를 제공할
  수도 있습니다.

- 골드: 큐레이팅된 영역이라고도 하는 이 최종 레이어는 실버 레이어에서
  소싱된 데이터를 저장합니다. 데이터는 특정 다운스트림 비즈니스 및 분석
  요구 사항을 충족하도록 구체화됩니다. 테이블은 일반적으로 성능 및
  유용성에 최적화된 데이터 모델의 개발을 지원하는 스타 스키마 디자인을
  따릅니다.

**목표**:

- Microsoft Fabric Lakehouse 내에서 Medallion Architecture의 원칙을
  이해합니다.

- 메달리온 레이어(Bronze, Silver, Gold)를 사용하여 구조화된 데이터 관리
  프로세스를 구현합니다.

- 원시 데이터를 검증되고 보강된 데이터로 변환하여 고급 분석 및 보고를
  수행할 수 있습니다.

- 데이터 보안, CI/CD 및 효율적인 데이터 쿼리를 위한 모범 사례를
  알아보세요.

&nbsp;

- OneLake 파일 탐색기를 사용하여 OneLake에 데이터를 업로드합니다.

- Fabric 노트북을 사용하여 OneLake에서 데이터를 읽고 델타 테이블로 다시
  씁니다.

- Fabric 노트북을 사용하여 Spark로 데이터를 분석하고 변환합니다.

- SQL을 사용하여 OneLake에서 데이터 사본 하나를 쿼리합니다.

- Azure Databricks를 사용하여 ADLS(Azure Data Lake Storage) Gen2 계정에
  델타 테이블을 만듭니다.

- ADLS에서 델타 테이블에 대한 OneLake 바로 가기를 만듭니다.

- Power BI를 사용하여 ADLS 바로 가기를 통해 데이터를 분석합니다.

- Azure Databricks를 사용하여 OneLake에서 델타 테이블을 읽고 수정합니다.

# 연습 1: 샘플 데이터를 Lakehouse로 가져오기

이 연습에서는 Microsoft Fabric을 사용하여 Lakehouse를 만들고 데이터를
로드하는 프로세스를 진행합니다.

작업:트레일

## **작업 1: Fabric 작업 영역 만들기**

이 작업에서는 Fabric 작업 공간을 만듭니다. 작업 영역에는 Lakehouse,
데이터 흐름, 데이터 팩터리 파이프라인, 노트북, Power BI 데이터 세트 및
보고서를 포함하여 이 Lakehouse 자습서에 필요한 모든 항목이 포함되어
있습니다.

1.  브라우저를 열고 주소 표시줄로 이동한 다음 다음 URL을 입력하거나
    붙여넣습니다:
    [https://app.fabric.microsoft.com/](https://app.fabric.microsoft.com/,)
    그런 다음 **Enter** 버튼을 누릅니다.

> ![A search engine window with a red box Description automatically
> generated with medium confidence](./media/image1.png)

2.  **Power BI 창**으로 돌아갑니다. Power BI 홈페이지의 왼쪽 탐색
    메뉴에서 **작업** 영역을 탐색하고 클릭합니다.

![](./media/image2.png)

3.  작업 영역 창에서 **+New workspace** 버튼을 클릭합니다**.**

> ![](./media/image3.png)

4.  오른쪽에 표시되는 **Create a workspace** 창에서 다음 세부 정보를
    입력하고 **Apply** 버튼을 클릭합니다.

[TABLE]

> ![](./media/image4.png)ㅎ

![A screenshot of a computer Description automatically
generated](./media/image5.png)

5.  배포가 완료될 때까지 기다립니다. 완료하는 데 2-3분이 걸립니다.

![](./media/image6.png)

## **작업 2: Lakehouse 만들기**

1.  In the **Power BI Fabric Lakehouse Tutorial-XX** page, click on the
    **Power BI** icon located at the bottom left and select **Data
    Engineering**. **Power BI Fabric Lakehouse Tutorial-XX** 페이지에서
    왼쪽 하단에 있는 **Power BI** 아이콘을 클릭하고 **Data
    Engineering**을 선택합니다.![](./media/image7.png)

2.  **Synapse Data Engineering 홈페이지**에서 **Lakehouse**를 선택하여
    Lakehouse를 만듭니다.

![](./media/image8.png)

![A screenshot of a computer Description automatically
generated](./media/image9.png)

3.  **New lakehouse** 대화 상자에서 **Name** 필드에 **wwilakehouse**를
    입력하고 **Create** 버튼을 클릭하여 새 Lakehouse를 엽니다.

> **참고**: **wwilakehouse** 앞에 있는 공간을 제거하세요.
>
> ![](./media/image10.png)
>
> ![A screenshot of a computer Description automatically
> generated](./media/image11.png)
>
> ![](./media/image12.png)

4.  **Successfully created SQL endpoint**.라는 알림이 표시됩니다.

> ![](./media/image13.png)

# 연습 2: Azure Databricks를 사용하여 메달리온 아키텍처 구현

## **작업 1: 브론즈 레이어 설정**

1.  **wwilakehouse** 페이지에서 파일 옆에 있는 추가 아이콘을 선택하고
    **New subfolder**를 선택합니다.

![](./media/image14.png)

2.  팝업에서 폴더 이름을 **bronze**로 입력하고 만들기를 선택합니다.

![A screenshot of a computer Description automatically
generated](./media/image15.png)

3.  이제 청동 파일 옆에 있는 추가 아이콘을 선택하고 **Upload**를 선택한
    다음 **파일을 업로드합니다**.

![A screenshot of a computer Description automatically
generated](./media/image16.png)

4.  **upload file**  창에서 **Upload file**라디오 버튼을 선택합니다.
    **Browse button** 버튼을 클릭하고 **C:\LabFiles**로 이동한 후,
    필요한 판매 데이터 파일(2019, 2020, 2021)을 선택하고 **Open** 버튼을
    클릭합니다.

그런 다음 **Upload**를 선택하여 Lakehouse의 새 'bronze' 폴더에 파일을
업로드합니다.

![A screenshot of a computer Description automatically
generated](./media/image17.png)

> ![A screenshot of a computer Description automatically
> generated](./media/image18.png)

5.  **Bronze**폴더를 클릭하여 파일이 성공적으로 업로드되었고 파일이
    반영되는지 확인합니다.

![A screenshot of a computer Description automatically
generated](./media/image19.png)

# 연습 3: Apache Spark를 사용한 데이터 변환 및 메달리온 아키텍처에서 SQL을 사용한 쿼리

## **작업 1: 데이터 변환 및 실버 델타 테이블에 로드**

**wwilakehouse** 페이지에서 명령 모음에 있는 **Open notebook**를
탐색하여 클릭한 다음 **New notebook**을 선택합니다.

![A screenshot of a computer Description automatically
generated](./media/image20.png)

1.  첫 번째 셀(현재는 코드 셀)을 선택한 다음, 오른쪽 상단의 동적 도구
    모음에서 **M↓** 버튼을 사용하여 셀을 마크다운 셀로 변환합니다.

![A screenshot of a computer Description automatically
generated](./media/image21.png)

2.  셀이 마크다운 셀로 변경되면 셀에 포함된 텍스트가 렌더링됩니다.

![A screenshot of a computer Description automatically
generated](./media/image22.png)

3.  🖉 (편집) 버튼을 사용하여 셀을 편집 모드로 전환하고, 모든 텍스트를
    바꾼 후 다음과 같이 마크다운을 수정합니다.

CodeCopy

\# Sales order data exploration

Use the code in this notebook to explore sales order data.

![A screenshot of a computer Description automatically
generated](./media/image23.png)

![A screenshot of a computer Description automatically
generated](./media/image24.png)

4.  셀 밖의 노트북 아무 곳이나 클릭하면 편집을 중단하고 렌더링된
    마크다운을 볼 수 있습니다.

![A screenshot of a computer Description automatically
generated](./media/image25.png)

5.  셀 출력 아래에 있는 + 코드 아이콘을 사용하여 노트북에 새로운 코드
    셀을 추가합니다.

![A screenshot of a computer Description automatically
generated](./media/image26.png)

6.  이제 노트북을 사용하여 청동 레이어의 데이터를 Spark DataFrame으로
    로드합니다.

노트북에서 간단한 주석 처리된 코드가 포함된 기존 셀을 선택합니다. 이 두
줄을 강조 표시하고 삭제합니다. 이 코드는 필요하지 않습니다.

*참고: 노트북을 사용하면 Python, Scala, SQL 등 다양한 언어로 코드를
실행할 수 있습니다. 이 연습에서는 PySpark와 SQL을 사용합니다. 마크다운
셀을 추가하여 서식 있는 텍스트와 이미지를 제공하여 코드를 문서화할 수도
있습니다.*

이를 위해 다음 코드를 입력하고 **Run**을 클릭하세요.

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

***참고:** 이 노트북에서 Spark 코드를 처음 실행하는 것이므로 Spark
세션을 시작해야 합니다. 즉, 첫 번째 실행은 완료하는 데 1분 정도 걸릴 수
있습니다. 이후 실행은 더 빨리 완료됩니다.*

![A screenshot of a computer Description automatically
generated](./media/image28.png)

7.  실행한 코드는 **bronze** 폴더에 있는 CSV 파일의 데이터를 Spark
    데이터프레임으로 로드한 다음, 데이터프레임의 처음 몇 행을
    표시했습니다.

> **참고**: 출력 창의 왼쪽 상단에 있는 … 메뉴를 선택하면 셀 출력 내용을
> 지우고, 숨기고, 크기를 자동으로 조정할 수 있습니다.

8.  이제 PySpark 데이터프레임을 사용하여 열을 추가하고 기존 열 중 일부의
    값을 업데이트하여 **데이터 검증 및 정리를 위한 열을 추가합니다.** +
    버튼을 사용하여 **새 코드 블록을 추가하고** 다음 코드를 셀에
    추가합니다:

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
> 코드의 첫 번째 줄은 PySpark에서 필요한 함수를 가져옵니다. 그런 다음
> 데이터프레임에 새 열을 추가하여 소스 파일 이름, 주문이 해당 회계연도
> 이전에 플래그가 지정되었는지 여부, 그리고 행이 생성되고 수정된 시점을
> 추적할 수 있습니다.
>
> 마지막으로, CustomerName 열이 null이거나 비어 있으면 해당 열을 "알 수
> 없음"으로 업데이트합니다.
>
> 그런 다음 \*\*▷ (*Run cell*)\*\* 버튼을 사용하여 셀을 실행하여 코드를
> 실행합니다.

![A screenshot of a computer Description automatically
generated](./media/image29.png)

![A screenshot of a computer Description automatically
generated](./media/image30.png)

9.  다음으로, Delta Lake 형식을 사용하여 sales 데이터베이스의
    **sales_silver** 테이블에 대한 스키마를 정의합니다. 새 코드 블록을
    만들고 다음 코드를 셀에 추가합니다:

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

   

10. **\*\*▷** (*Run cell*)\*\*버튼을 사용하여 셀을 실행하여 코드를
    실행합니다.

11. Lakehouse 탐색기 창의 테이블 섹션에서 **…**을 선택하고 **Refresh를**
    선택합니다. 이제 새 **sales_silver** 테이블이 나열됩니다. ▲(삼각형
    아이콘)는 해당 테이블이 델타 테이블임을 나타냅니다.

> **참고:** 새 표가 보이지 않으면 몇 초간 기다린 후 다시 **Refresh**을
> 선택하거나 브라우저 탭 전체를 새로 고침하세요.
>
> ![A screenshot of a computer Description automatically
> generated](./media/image31.png)
>
> ![A screenshot of a computer Description automatically
> generated](./media/image32.png)

12. 이제 델타 테이블에 대해 upsert 작업을 수행하여 특정 조건에 따라 기존
    레코드를 업데이트하고 일치하는 레코드가 없으면 새 레코드를
    삽입합니다. 새 코드 블록을 추가하고 다음 코드를 붙여넣습니다:

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

13. **\*\*▷** (*Run cell*)\*\*버튼을 사용하여 셀을 실행하여 코드를
    실행합니다.

![A screenshot of a computer Description automatically
generated](./media/image33.png)

이 작업은 특정 열의 값을 기준으로 테이블의 기존 레코드를 업데이트하고,
일치하는 레코드가 없으면 새 레코드를 삽입할 수 있기 때문에 중요합니다.
이는 기존 레코드와 새 레코드에 대한 업데이트가 포함된 소스 시스템에서
데이터를 로드할 때 일반적으로 필요한 작업입니다.

이제 실버 델타 테이블에 추가 변환 및 모델링을 위한 데이터가
준비되었습니다.

브론즈 레이어에서 데이터를 가져와 변환하고 실버 델타 테이블에 로드하는
작업이 성공적으로 완료되었습니다. 이제 새 노트북을 사용하여 데이터를
추가로 변환하고, 스타 스키마로 모델링하고, 골드 델타 테이블에 로드해
보겠습니다.

*이 모든 작업을 하나의 노트북에서 수행할 수도 있지만, 이 연습에서는
브론즈에서 실버로, 그리고 실버에서 골드로 데이터를 변환하는 과정을
보여주기 위해 별도의 노트북을 사용합니다. 이는 디버깅, 문제 해결 및
재사용에 도움이 될 수 있습니다.*

## **작업 2: 골드 델타 테이블에 데이터 로드**

1.  Fabric Lakehouse Tutorial-29 홈페이지로 돌아갑니다.

> ![A screenshot of a computer Description automatically
> generated](./media/image34.png)

2.  **Wwilakehouse 선택합니다.**

![A screenshot of a computer Description automatically
generated](./media/image35.png)

3.  Lakehouse탐색기 창에서 **Tables** 섹션에 **sales_silver** 테이블이
    나열되어 있는 것을 볼 수 있습니다.

![A screenshot of a computer Description automatically
generated](./media/image36.png)

4.  이제 **Transform data for Gold**라는 새 노트북을 만듭니다. 이를 위해
    명령줄에서 **Open notebook** 드롭다운을 찾아 클릭한 다음 **New
    notebook**을 선택합니다.

![A screenshot of a computer Description automatically
generated](./media/image37.png)

5.  기존 코드 블록에서 보일러플레이트 텍스트를 제거하고 **다음 코드를
    추가하여** 데이터 프레임에 데이터를 로드하고 스타 스키마를 구축한
    다음 실행합니다:

> CodeCopy

\# Load data to the dataframe as a starting point to create the gold
layer

df = spark.read.table("wwilakehouse.sales_silver")

\# Display the first few rows of the dataframe to verify the data

df.show()

![A screenshot of a computer Description automatically
generated](./media/image38.png)

6.  다음으로, **새 코드 블록을 추가하고** 다음 코드를 붙여넣어 날짜 차원
    테이블을 만들고 실행합니다:

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

**참고**: 언제든지 display(df)명령을 실행하여 작업 진행 상황을 확인할 수
있습니다. 이 경우, 'display(dfdimDate_gold)' 명령을 실행하여
dimDate_gold 데이터프레임의 내용을 확인합니다.

7.  새 코드 블록에서 **다음 코드를 추가하고 실행하여** 날짜 차원
    **dimdate_gold**에 대한 데이터 프레임을 만듭니다:

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

8.  데이터를 변환할 때 노트북에서 어떤 일이 일어나는지 이해하고 확인할
    수 있도록 코드를 새 코드 블록으로 분리합니다. 또 다른 새 코드 블록에
    **다음 코드를 추가하고 실행하여** 새 데이터가 들어올 때 날짜 차원을
    업데이트합니다:

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

> 날짜 차원이 모두 설정되었습니다.

![A screenshot of a computer Description automatically
generated](./media/image43.png)

## **작업3: 고객 차원 만들기**

1.  고객 차원 테이블을 작성하려면 **새 코드 블록을 추가하고** 다음
    코드를 붙여넣고 실행합니다.

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

2.  새 코드 블록에서 **다음 코드를 추가하고 실행하여** 중복된 고객을
    삭제하고, 특정 열을 선택하고, "CustomerName" 열을 분할하여 "First"와
    "Last" 이름 열을 만듭니다:

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

중복 항목 삭제, 특정 열 선택, "CustomerName" 열을 분할하여 "First"과 "
Last " 열 생성 등 다양한 변환을 수행하여 새로운 DataFrame
dfdimCustomer_silver를 생성했습니다. 그 결과, "CustomerName" 열에서
추출된 별도의 "First"과 "Last" 열이 포함된 정리되고 구조화된 고객
데이터가 포함된 DataFrame이 생성됩니다.

![A screenshot of a computer Description automatically
generated](./media/image47.png)

3.  다음으로, **고객의 ID 열을 생성하겠습니다**. 새 코드 블록에 다음을
    붙여넣고 실행합니다:

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

4.  이제 새 데이터가 들어오면 고객 테이블이 최신 상태로 유지되도록 할 수
    있습니다. **새 코드 블록에** 다음을 붙여넣고 실행합니다:

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

5.  이제 이 **단계를 반복하여 제품 차원을 생성합니다**. 새 코드 블록에
    다음을 붙여넣고 실행합니다.

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

6.  **Product_silver** 데이터프레임을 생성하기 위해 **또 다른 코드
    블록을** 추가합니다.

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

7.  이제 **dimProduct_gold table**의 ID를 생성합니다. 새 코드 블록에
    다음 구문을 추가하고 실행합니다:

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

이는 표의 현재 데이터를 기반으로 사용 가능한 다음 제품 ID를 계산하고,
이러한 새로운 ID를 제품에 할당한 다음 업데이트된 제품 정보를 표시합니다.

![A screenshot of a computer Description automatically
generated](./media/image57.png)

8.  다른 차원에서 수행한 것과 유사하게 새 데이터가 들어오면 제품
    테이블이 최신 상태로 유지되도록 해야 합니다. **새 코드 블록에**
    다음을 붙여넣고 실행합니다:

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

**이제 차원을 구축했으므로 마지막 단계는 사실 표를 만드는 것입니다.**

9.  **새 코드 블록에** 다음 코드를 붙여넣고 실행하여 **fact table**을
    만듭니다.

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

10. **새 코드 블록에** 다음 코드를 붙여넣고 실행하여 판매 데이터와 고객
    및 제품 정보를 결합하는 **새 데이터 프레임**을 만듭니다. 여기에는
    고객 ID, 품목 ID, 주문 날짜, 수량, 단가, 세금이 포함됩니다:

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

1.  이제 **새 코드 블록에서** 다음 코드를 실행하여 판매 데이터가 최신
    상태로 유지되도록 합니다:

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

여기서는 Delta Lake의 병합 작업을 사용하여 factsales_gold 테이블을
새로운 판매 데이터(dffactSales_gold)로 동기화하고 업데이트합니다. 이
작업은 기존 데이터(silver 테이블)와 새 데이터(updates DataFrame)의 주문
날짜, 고객 ID, 품목 ID를 비교하여 일치하는 레코드를 업데이트하고 필요에
따라 새 레코드를 삽입합니다.

![A screenshot of a computer Description automatically
generated](./media/image66.png)

이제 보고 및 분석에 사용할 수 있는 큐레이팅되고 모델링된 골드 레이어가
생겼습니다.

# 연습 4: Azure Databricks와 Azure Data Lake Storage(ADLS) Gen 2 간 연결 설정

이제 Azure Databricks를 사용하여 Azure Data Lake Storage(ADLS) Gen2
계정의 도움을 받아 델타 테이블을 만들어 보겠습니다. 그런 다음 ADLS에서
델타 테이블에 대한 OneLake 바로 가기를 만들고, Power BI를 사용하여 ADLS
바로 가기를 통해 데이터를 분석해 보겠습니다.

## **작업 0: Azure 패스를 사용하고 Azure 구독을 활성화하기**

1.  다음 링크로 이동합니다 !!https://www.microsoftazurepass.com/!!
    그리고 **Start** 버튼을 클릭합니다.

![](./media/image67.png)

2.  Microsoft 로그인 페이지에서 **Tenant ID**를 입력하고 **Next**를
    클릭합니다.

![](./media/image68.png)

3.  다음 페이지에서 비밀번호를 입력하고 **Sign In**을 클릭합니다.

![](./media/image69.png)

![A screenshot of a computer error Description automatically
generated](./media/image70.png)

4.  로그인한 후 Microsoft Azure 페이지에서 **Confirm Microsoft
    Account**탭을 클릭합니다.

![](./media/image71.png)

5.  다음 페이지에서 프로모션 코드와 Captcha 문자를 입력하고 **Submit**을
    클릭하세요.

![A screenshot of a computer Description automatically
generated](./media/image72.png)

![A screenshot of a computer error Description automatically
generated](./media/image73.png)

6.  프로필 페이지에서 프로필 세부 정보를 입력하고 **Sign up**을
    클릭하세요.

7.  메시지가 표시되면 다중 인증에 가입한 후 다음 링크로 이동하여 Azure
    Portal에 로그인하세요!! <https://portal.azure.com/#home>!!

![](./media/image74.png)

8.  검색창에 구독을 입력하고 **Services.** 아래에 있는 구독 아이콘을
    클릭합니다.

![A screenshot of a computer Description automatically
generated](./media/image75.png)

9.  Azure Pass를 성공적으로 사용하면 구독 ID가 생성됩니다.

![](./media/image76.png)

## **작업 1: Azure Data Storage 계정 만들기**

1.  Azure 자격 증명을 사용하여 Azure Portal에 로그인합니다.

2.  홈페이지 왼쪽 포털 메뉴에서 **Storage accounts** 을 선택하면 저장소
    계정 목록이 표시됩니다. 포털 메뉴가 보이지 않으면 메뉴 버튼을
    선택하여 킵니다.

![A screenshot of a computer Description automatically
generated](./media/image77.png)

3.  **Storage accounts** 페이지에서 **Create**를 선택합니다.

![A screenshot of a computer Description automatically
generated](./media/image78.png)

4.  Basics 탭에서 리소스 그룹을 선택하면 저장소 계정에 대한 필수 정보를
    제공합니다:

[TABLE]

다른 설정은 그대로 두고 **Review + create**를 선택하여 기본 옵션을
수락하고 계정의 유효성을 검사하고 생성합니다.

참고: 아직 리소스 그룹을 생성하지 않은 경우 "**Create new**"를 클릭하고
스토리지 계정에 대한 새 리소스를 만들 수 있습니다.

![](./media/image79.png)

![](./media/image80.png)

![A screenshot of a computer Description automatically
generated](./media/image81.png)

![A screenshot of a computer Description automatically
generated](./media/image82.png)

![A screenshot of a computer Description automatically
generated](./media/image83.png)

5.  **Review + create** 탭으로 이동하면 Azure에서 선택한 저장소 계정
    설정에 대한 유효성 검사를 실행합니다. 유효성 검사에 통과하면 저장소
    계정 만들기를 진행할 수 있습니다.

유효성 검사에 실패하면 포털은 어떤 설정을 수정해야 하는지 알려줍니다.

![A screenshot of a computer Description automatically
generated](./media/image84.png)

![A screenshot of a computer Description automatically
generated](./media/image85.png)

이제 Azure 데이터 저장소 계정이 성공적으로 생성되었습니다.

6.  페이지 상단의 검색창에서 검색하여 스토리지 계정 페이지로 이동한 후
    새로 만든 스토리지 계정을 선택합니다.

![A screenshot of a computer Description automatically
generated](./media/image86.png)

7.  저장소 계정 페이지에서 왼쪽 탐색 창의 **Data storage**아래에 있는
    **Containers**로 이동하여 !!medalion1!!이라는 이름으로 새 컨테이너를
    만들고 **Create** 버튼을 클릭합니다.

 

![A screenshot of a computer Description automatically
generated](./media/image87.png)

8.  이제 **storage account**페이지로 돌아가 왼쪽 탐색 메뉴에서
    **Endpoints**를 선택하세요. 아래로 스크롤하여 **Primary endpoint
    URL**을 복사하여 메모장에 붙여넣으세요. 바로가기를 만들 때 도움이 될
    것입니다.![](./media/image88.png)

9.  마찬가지로, 같은 탐색 패널에서 **Access keys**로 이동합니다.

![A screenshot of a computer Description automatically
generated](./media/image89.png)

## **작업 2: Delta 테이블 만들기, 바로 가기 만들기, Lakehouse의 데이터 분석**

1.  lakehouse에서 파일 옆에 있는 줄임표**(…)**를 선택한 다음 **New
    shortcut**를 선택합니다.

![](./media/image90.png)

2.  **New shortcut**화면에서 **Azure Data Lake Storage Gen2** 타일을
    선택합니다.

![Screenshot of the tile options in the New shortcut
screen.](./media/image91.png)

3.  바로가기에 대한 연결 세부 정보를 지정합니다.

[TABLE]

4.  **Next**를 클릭합니다.

![A screenshot of a computer Description automatically
generated](./media/image92.png)

5.  Azure storage container와의 연결이 설정됩니다. 저장소를 선택하고
    **Next**버튼을 클릭하세요.

![A screenshot of a computer Description automatically
generated](./media/image93.png)

![A screenshot of a computer Description automatically
generated](./media/image94.png)![A screenshot of a computer Description
automatically generated](./media/image95.png)

6.  Wizard가 실행되면 **Files**을 선택하고 **bronze** 파일에서
    **"..."**을 선택합니다.

![A screenshot of a computer Description automatically
generated](./media/image96.png)

7.  **load to tables**과 **new table**. **load to tables**을 선택합니다.

![](./media/image97.png)

8.  팝업 창에서 테이블 이름을 **bronze_01**로 입력하고 파일 유형을
    **parquet**로 선택합니다.

![A screenshot of a computer Description automatically
generated](./media/image98.png)

![A screenshot of a computer Description automatically
generated](./media/image99.png)

![A screenshot of a computer Description automatically
generated](./media/image100.png)

![A screenshot of a computer Description automatically
generated](./media/image101.png)

9.  **bronze_01** 파일이 이제 파일에서 표시됩니다.

![A screenshot of a computer Description automatically
generated](./media/image102.png)

10. 다음으로, **bronze** 파일에서 "..."을 선택합니다. **load to
    tables**와 **existing table**을 선택합니다.

![A screenshot of a computer Description automatically
generated](./media/image103.png)

11. 기존 테이블 이름을 **dimcustomer_gold**로 지정하고, 파일 형식을
    **parquet**로 선택한 후 **load**를 선택하세요.

![A screenshot of a computer Description automatically
generated](./media/image104.png)

![A screenshot of a computer Description automatically
generated](./media/image105.png)

## **작업 3: 골드 레이어를 사용하여 보고서 생성을 위한 의미 모델 생성**

이제 작업 공간에서 골드 레이어를 사용하여 보고서를 생성하고 데이터를
분석할 수 있습니다. 작업 공간에서 의미 모델에 직접 액세스하여 보고를
위한 관계 및 측정값을 생성할 수 있습니다.

Lakehouse를 생성할 때 자동으로 생성되는 **기본 의미 모델을** 사용할 수
없습니다. Lakehouse 탐색기에서 이 랩에서 생성한 골드 테이블을 포함하는
새 의미 모델을 생성해야 합니다.

1.  작업 공간에서 **wwilakehouse l**akehouse로 이동합니다. 그런 다음
    lakehouse 탐색기 뷰의 리본 메뉴에서 **New semantic model**을
    선택합니다.

![A screenshot of a computer Description automatically
generated](./media/image106.png)

2.  팝업에서 새 의미 모델에 **DatabricksTutorial**이라는 이름을 지정하고
    작업 공간으로 **Fabric Lakehouse Tutorial-29**를 선택합니다.

![](./media/image107.png)

3.  다음으로, 아래로 스크롤하여 의미 모델에 포함할 모든 항목을 선택하고
    **Confirm**을 선택합니다.

이렇게 하면 Fabric에서 의미 모델이 열리고 여기서 관계와 측정값을 생성할
수 있습니다(아래 그림 참조).

![A screenshot of a computer Description automatically
generated](./media/image108.png)

여기에서 사용자 또는 데이터 팀의 다른 구성원이lakehouse의 데이터를
기반으로 보고서와 대시보드를 만들 수 있습니다. 이 보고서는lakehouse의
골드 레이어에 직접 연결되므로 항상 최신 데이터를 반영합니다.

# 연습 5: Azure Databricks를 사용하여 데이터 수집 및 분석

1.  Power BI 서비스에서lakehouse로 이동하여 **Get data** 를 선택한 다음
    **New data pipeline**을 선택합니다.

![Screenshot showing how to navigate to new data pipeline option from
within the UI.](./media/image109.png)

1.  **새 파이프라인** 프롬프트에서 새 파이프라인의 이름을 입력한 다음
    **Create**를 선택합니다. **IngestDatapipeline01**

![](./media/image110.png)

2.  이 연습에서는 데이터 소스로 **NYC Taxi - Green** 샘플 데이터를
    선택합니다.

![A screenshot of a computer Description automatically
generated](./media/image111.png)

3.  미리보기 화면에서 **Next**을 선택합니다.

![A screenshot of a computer Description automatically
generated](./media/image112.png)

4.  데이터 대상에서 OneLake Delta 테이블 데이터를 저장하는 데 사용할
    테이블 이름을 선택하세요. 기존 테이블을 선택하거나 새 테이블을 만들
    수 있습니다. 이 실습에서는 **load into new table을** 선택하고
    **Next**를 선택하세요.

![A screenshot of a computer Description automatically
generated](./media/image113.png)

5.  **Review + Save**  화면에서 **Start data transfer immediately**을
    선택한 다음 **Save + Run**을 선택합니다.

![Screenshot showing how to enter table name.](./media/image114.png)

![A screenshot of a computer Description automatically
generated](./media/image115.png)

6.  작업이 완료되면 Lakehouse로 이동하여 /Tables 아래에 나열된 델타
    테이블을 확인합니다.

![A screenshot of a computer Description automatically
generated](./media/image116.png)

7.  탐색기 보기에서 테이블 이름을 마우스 오른쪽 버튼으로 클릭하고
    **Properties을** 선택하여 Azure Blob Filesystem(ABFS) 경로를 델타
    테이블에 복사합니다.

![A screenshot of a computer Description automatically
generated](./media/image117.png)

8.  Azure Databricks 노트북을 열고 코드를 실행합니다.

olsPath = "**abfss://\<replace with workspace
name\>@onelake.dfs.fabric.microsoft.com/\<replace with item
name\>.Lakehouse/Tables/nycsample**"

df=spark.read.format('delta').option("inferSchema","true").load(olsPath)

df.show(5)

*참고: 굵게 표시된 파일 경로를 복사한 파일 경로로 바꿉니다.*

![](./media/image118.png)

9.  필드 값을 변경하여 델타 테이블 데이터를 업데이트합니다.

%sql

update delta.\`abfss://\<replace with workspace
name\>@onelake.dfs.fabric.microsoft.com/\<replace with item
name\>.Lakehouse/Tables/nycsample\` set vendorID = 99999 where vendorID
= 1;

*참고: 굵게 표시된 파일 경로를 복사한 파일 경로로 바꿉니다.*

![A screenshot of a computer Description automatically
generated](./media/image119.png)

# 연습 6: 리소스 정리

이 연습에서는 Microsoft Fabric Lakehouse에서 메달리온 아키텍처를 만드는
방법을 알아보았습니다.

Lakehouse를 살펴보았으면 이 연습을 위해 만든 작업 공간을 삭제할 수
있습니다.

1.  왼쪽 탐색 메뉴에서 **Fabric Lakehouse Tutorial-29** 작업 공간을
    선택하세요. 작업 공간 항목 보기가 열립니다.

![A screenshot of a computer Description automatically
generated](./media/image120.png)

2.  작업 공간 이름 아래에서 ... 옵션을 선택하고 **Workspace settings**을
    선택합니다. 

![A screenshot of a computer Description automatically
generated](./media/image121.png)

3.  아래로 스크롤하여 **Remove this workdspace.**

![A screenshot of a computer Description automatically
generated](./media/image122.png)

4.  팝업되는 경고에서 **Delete를** 클릭합니다.

![A white background with black text Description automatically
generated](./media/image123.png)

5.  다음 실습으로 진행하기 전에 작업공간이 삭제되었다는 알림을
    기다립니다.

![A screenshot of a computer Description automatically
generated](./media/image124.png)

**요약**:

이 랩은 참가자가 노트북을 사용하여 Microsoft Fabric Lakehouse에서
메달리온 아키텍처를 빌드하는 방법을 안내합니다. 주요 단계에는 작업 공간
설정, Lakehouse 설정, 초기 수집을 위해 브론즈 계층에 데이터 업로드,
구조적 처리를 위해 실버 델타 테이블로 변환, 고급 분석을 위해 골드 델타
테이블로 추가 세분화, 의미 체계 모델 탐색, 통찰력 있는 분석을 위한
데이터 관계 생성이 포함됩니다.

## 
