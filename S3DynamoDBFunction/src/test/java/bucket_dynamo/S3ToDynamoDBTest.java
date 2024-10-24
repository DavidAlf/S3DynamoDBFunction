package bucket_dynamo;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

class S3ToDynamoDBTest {

        @Mock
        private S3Client s3Client;

        @Mock
        private DynamoDbClient dynamoDbClient;

        @Mock
        private Context context;

        private S3ToDynamoDB handler;

        private static final String TARGET_BUCKET = "periferia-s3-demo";
        // private static final String TARGET_BUCKET = "s3-to-dynamo-jdao";
        private static final String TARGET_FOLDER = "event-jdao/Documentos/";

        @BeforeEach
        void setUp() {
                MockitoAnnotations.openMocks(this);

                handler = new S3ToDynamoDB();

                // Use reflection to set mocked clients
                setField(handler, "s3Client", s3Client);
                setField(handler, "dynamoDbClient", dynamoDbClient);
        }

        @Test
        void testHandleRequest() {
                String jsonContent = "[{\"name\": \"John\", \"lastName\": \"Doe\", \"status\": \"active\"}," +
                                "{\"name\": \"Jane\", \"lastName\": \"Smith\", \"status\": \"inactive\"}," +
                                "{\"name\": \"Carlos\", \"lastName\": \"Gonzalez\", \"status\": \"pending\"}]";

                S3EventNotification.S3EventNotificationRecord eventRecord = new S3EventNotification.S3EventNotificationRecord(
                                "us-east-1",
                                "ObjectCreated:Put",
                                "aws:s3",
                                "2023-01-01T00:00:00.000Z",
                                "1.0",
                                new S3EventNotification.RequestParametersEntity("sourceIPAddress"),
                                new S3EventNotification.ResponseElementsEntity("x-amz-request-id", "x-amz-id-2"),
                                new S3EventNotification.S3Entity(
                                                "configurationId",
                                                new S3EventNotification.S3BucketEntity(TARGET_BUCKET,
                                                                new S3EventNotification.UserIdentityEntity(
                                                                                "principalId"),
                                                                "arn:aws:s3:::" + TARGET_BUCKET),
                                                new S3EventNotification.S3ObjectEntity(
                                                                TARGET_FOLDER + "test-object.txt", 1024L, "etag",
                                                                "versionId", "sequencer"),
                                                "1.0"),
                                new S3EventNotification.UserIdentityEntity("principalId"));
                S3Event event = new S3Event(Collections.singletonList(eventRecord));

                GetObjectResponse objectResponse = GetObjectResponse.builder()
                                .contentLength((long) jsonContent.length())
                                .lastModified(Instant.parse("2023-01-01T00:00:00Z"))
                                .build();

                ResponseInputStream<GetObjectResponse> responseInputStream = new ResponseInputStream<>(
                                objectResponse,
                                new ByteArrayInputStream(jsonContent.getBytes()));

                when(s3Client.getObject(any(GetObjectRequest.class))).thenReturn(responseInputStream);

                when(dynamoDbClient.createTable(any(CreateTableRequest.class))).thenReturn(null);

                DescribeTableResponse describeResponse = DescribeTableResponse.builder()
                                .table(TableDescription.builder().tableStatus("ACTIVE").build())
                                .build();

                when(dynamoDbClient.describeTable(any(DescribeTableRequest.class)))
                                .thenReturn(describeResponse);

                when(dynamoDbClient.putItem(any(PutItemRequest.class))).thenReturn(null);

                Map<String, String> result = handler.handleRequest(event, context);

                verify(s3Client).getObject(any(GetObjectRequest.class));

                verify(dynamoDbClient, times(3)).putItem(any(PutItemRequest.class));

        }

        private void setField(Object target, String fieldName, Object value) {
                try {
                        java.lang.reflect.Field field = target.getClass().getDeclaredField(fieldName);
                        field.setAccessible(true);
                        field.set(target, value);
                } catch (NoSuchFieldException | IllegalAccessException e) {
                        throw new RuntimeException(e);
                }
        }
}
