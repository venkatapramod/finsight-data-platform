from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml import Pipeline

def main():
    spark = SparkSession.builder \
        .appName("FinSight-MLlib-Fraud-Detection") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print("Reading data for model training...")
    df = spark.read.json("/finsight/raw/transactions")

    # Drop null records
    df = df.filter(col("type").isNotNull())

    # Encode categorical field
    indexer = StringIndexer(inputCol="type", outputCol="typeIndexed", handleInvalid="keep")

    # Assemble feature vector
    feature_cols = ["typeIndexed", "amount", "oldbalanceOrg", "newbalanceOrig", "oldbalanceDest", "newbalanceDest"]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

    # Use LogisticRegression to avoid binning limitations on tiny datasets
    lr = LogisticRegression(labelCol="isFraud", featuresCol="features", maxIter=10)

    # Build and execute pipeline
    pipeline = Pipeline(stages=[indexer, assembler, lr])

    print("Training Logistic Regression model...")
    model = pipeline.fit(df)

    print("Evaluating model performance...")
    predictions = model.transform(df)

    evaluator = BinaryClassificationEvaluator(labelCol="isFraud", metricName="areaUnderROC")
    auc = evaluator.evaluate(predictions)
    print(f"Model AUC Performance: {auc:.4f}")

    # Save model back to HDFS
    model_path = "/finsight/models/fraud_rf_model"
    print(f"Saving model to HDFS: {model_path}")
    model.write().overwrite().save(model_path)

    print("ML Pipeline execution complete!")
    spark.stop()

if __name__ == "__main__":
    main()
