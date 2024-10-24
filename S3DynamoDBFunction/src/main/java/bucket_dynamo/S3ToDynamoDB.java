package bucket_dynamo;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification.S3EventNotificationRecord;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceInUseException;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

public class S3ToDynamoDB implements RequestHandler<S3Event, Map<String, String>> {
    private static final Logger logger = LoggerFactory.getLogger(S3ToDynamoDB.class);

    private S3Client s3Client;
    private DynamoDbClient dynamoDbClient;
    Region region;

    private static final String DYNAMODB_TABLE_NAME = "REGISTROS_S3_JDAO";
    private static final String KEY_ATTRIBUTE_VALUE = "idRegistros";
    private static final String TARGET_BUCKET = "periferia-s3-demo";
    // private static final String TARGET_BUCKET = "s3-to-dynamo-jdao";
    private static final String TARGET_FOLDER = "event-jdao/Documentos/";
    private static final long READ_CAPACITY_UNITS = 10L;
    private static final long WRITE_CAPACITY_UNITS = 10L;
    private static final ObjectMapper objectMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    public S3ToDynamoDB() {
        this.s3Client = S3Client.builder().build();
        this.dynamoDbClient = DynamoDbClient.builder().build();

        region = Region.US_EAST_1;
    }

    @Override
    public Map<String, String> handleRequest(S3Event s3event, Context context) {
        logger.info("handleRequest: ");
        Map<String, String> result = null;
        String message = "";

        if (s3event == null || s3event.getRecords().isEmpty()) {
            message = "Error";
            return Map.of(message, "No records found");
        }

        result = s3event.getRecords().stream()
                .filter(this::isValidRecord)
                .filter(this::isCreateTable)
                .map(this::processRecord)
                .map(this::insertDynamo)
                .reduce((a, b) -> b)
                .orElse(Map.of(message, "Registro no válido"));

        return result;
    }

    private boolean isValidRecord(S3EventNotificationRecord eventRecord) {
        logger.info("isValidRecord: ");
        String srcBucket = eventRecord.getS3().getBucket().getName();
        String srcKey = eventRecord.getS3().getObject().getUrlDecodedKey();

        return srcBucket.equals(TARGET_BUCKET) && srcKey.startsWith(TARGET_FOLDER);
    }

    private boolean isCreateTable(S3EventNotificationRecord eventRecord) {
        logger.info("createTable: ");

        try {
            CreateTableRequest request = CreateTableRequest.builder()
                    .attributeDefinitions(AttributeDefinition.builder()
                            .attributeName(KEY_ATTRIBUTE_VALUE)
                            .attributeType(ScalarAttributeType.S)
                            .build())
                    .keySchema(KeySchemaElement.builder()
                            .attributeName(KEY_ATTRIBUTE_VALUE)
                            .keyType(KeyType.HASH)
                            .build())
                    .provisionedThroughput(ProvisionedThroughput.builder()
                            .readCapacityUnits(READ_CAPACITY_UNITS)
                            .writeCapacityUnits(WRITE_CAPACITY_UNITS)
                            .build())
                    .tableName(DYNAMODB_TABLE_NAME)
                    .build();

            dynamoDbClient.createTable(request);

            return waitForTableToExist(DYNAMODB_TABLE_NAME);

        } catch (ResourceInUseException e) {
            logger.info("[SUCCESS] Tabla ya existe: {}", DYNAMODB_TABLE_NAME);
            return true;
        } catch (DynamoDbException e) {
            logger.error("[ERROR] createTable: {}", e.getMessage());
            return false;
        }
    }

    private boolean waitForTableToExist(String tableName) {
        logger.info("waitForTableToExist: ");
        int maxRetries = 10;
        int retryDelay = 2000;

        for (int i = 0; i < maxRetries; i++) {
            try {
                DescribeTableRequest tableRequest = DescribeTableRequest.builder()
                        .tableName(tableName)
                        .build();

                DescribeTableResponse response = dynamoDbClient.describeTable(tableRequest);

                if (response.table().tableStatusAsString().equals("ACTIVE")) {
                    return true;
                }

                Thread.sleep(retryDelay);
            } catch (DynamoDbException e) {
                logger.warn("Esperando que la tabla exista. Intento {}", i + 1);
                try {
                    Thread.sleep(retryDelay);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                logger.error("[ERROR] La espera fue interrumpida {}", ie.getMessage());
                return false;
            }
        }

        logger.error("La tabla no está activa después de varios intentos.");
        return false;
    }

    private List<Map<String, AttributeValue>> processRecord(S3EventNotificationRecord eventRecord) {
        logger.info("processRecord: ");
        String srcBucket = eventRecord.getS3().getBucket().getName();
        String srcKey = eventRecord.getS3().getObject().getUrlDecodedKey();

        logger.info("srcBucket: {}, srcKey: {}", srcBucket, srcKey);

        try {

            List<Map<String, AttributeValue>> listMaps = new ArrayList<>();

            getObjectResponse(srcBucket, srcKey, listMaps);

            logger.info("respuesta {}", listMaps.size());

            return listMaps;
        } catch (Exception e) {
            logger.error("[ERROR] procesando: {}", srcKey, e);
            return new ArrayList<>();
        }
    }

    private void getObjectResponse(String srcBucket, String srcKey, List<Map<String, AttributeValue>> listMaps) {
        logger.info("getObjectResponse: ");
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(srcBucket)
                .key(srcKey)
                .build();

        try (ResponseInputStream<GetObjectResponse> inputStream = s3Client.getObject(getObjectRequest)) {
            List<MsnObject> msnObjects = objectMapper.readValue(inputStream,
                    objectMapper.getTypeFactory().constructCollectionType(List.class, MsnObject.class));

            String lastModified = inputStream.response().lastModified().toString();
            String contentLength = String.valueOf(inputStream.response().contentLength());

            msnObjects.stream()
                    .map(msnObject -> createFileItem(srcBucket, srcKey, msnObject, lastModified, contentLength))
                    .forEach(listMaps::add);

        } catch (Exception e) {
            logger.error("[ERROR] leyendo la estructura del archivo: ", e);
            throw new RuntimeException("Fallo proceso de lectura del archivo", e);
        }
    }

    private Map<String, AttributeValue> createFileItem(String srcBucket, String srcKey, MsnObject msnObject,
            String lastModified, String contentLength) {
        logger.info("createFileItem: ");
        String uniqueId = UUID.randomUUID().toString();

        return Map.of(
                KEY_ATTRIBUTE_VALUE, AttributeValue.builder().s(uniqueId).build(),
                "bucket", AttributeValue.builder().s(srcBucket).build(),
                "document", AttributeValue.builder().s(srcKey).build(),
                "insertedAt", AttributeValue.builder().s(Instant.now().toString()).build(),
                "name", AttributeValue.builder().s(msnObject.getName()).build(),
                "lastName", AttributeValue.builder().s(msnObject.getLastName()).build(),
                "status", AttributeValue.builder().s(msnObject.getStatus()).build(),
                "lastModified", AttributeValue.builder().s(lastModified).build(),
                "size", AttributeValue.builder().n(contentLength).build());
    }

    private Map<String, String> insertDynamo(List<Map<String, AttributeValue>> fileItem) {
        logger.info("insertDynamo: ");
        String msnFinal = "ERROR";
        int cont = 0;
        try {
            for (Map<String, AttributeValue> item : fileItem) {
                AttributeValue keyValue = item.get(KEY_ATTRIBUTE_VALUE);
                logger.info("insertDynamo keyvalue: {} ", keyValue);

                PutItemRequest putItemRequest = PutItemRequest.builder()
                        .tableName(DYNAMODB_TABLE_NAME)
                        .item(item)
                        .build();

                if (keyValue != null) {
                    dynamoDbClient.putItem(putItemRequest);
                    cont++;
                }
            }

            if (cont == fileItem.size()) {
                msnFinal = "SUCCESS";
            }

            return Map.of(msnFinal, "Procesados: " + cont);

        } catch (DynamoDbException e) {
            msnFinal = "ERROR DYNAMO";
            logger.error("[{}] insertando en DynamoDB {}", msnFinal, e.getMessage());
            return Map.of(msnFinal, "Procesados: " + cont);
        } catch (Exception e) {
            msnFinal = "ERROR";
            logger.error("[{}] general {}", msnFinal, e.getMessage());
            return Map.of(msnFinal, "Procesados: " + cont);
        }
    }
}
