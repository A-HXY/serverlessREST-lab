import { APIGatewayProxyHandlerV2 } from "aws-lambda";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient,GetCommand,QueryCommand, } from "@aws-sdk/lib-dynamodb";

const ddbDocClient = createDDbDocClient();

export const handler: APIGatewayProxyHandlerV2 = async (event, context) => {     
  try {
    console.log("[EVENT]", JSON.stringify(event));
    const pathParameters  = event?.pathParameters;
    const queryParams = event?.queryStringParameters;

    const movieId = pathParameters?.movieId ? parseInt(pathParameters.movieId) : undefined;

    if (!movieId) {
      return {
        statusCode: 404,
        headers: {
          "content-type": "application/json",
        },
        body: JSON.stringify({ Message: "Missing movie Id" }),
      };
    }

    const commandOutput = await ddbDocClient.send(
      new GetCommand({
        TableName: process.env.TABLE_NAME,
        Key: { id: movieId },
      })
    );
    console.log("GetCommand response: ", commandOutput);

    if (!commandOutput.Item) {
      return {
        statusCode: 404,
        headers: {
          "content-type": "application/json",
        },
        body: JSON.stringify({ Message: "Invalid movie Id" }),
      };
    }

    const responseBody: any = {
      data: commandOutput.Item,
    };

    if (queryParams?.cast === "true") {
      console.log(`Fetching cast for movieId: ${movieId}`);
      const castData = await getMovieCastData(movieId);
      responseBody.cast = castData;
    }

    // Return Response
    return {
      statusCode: 200,
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify(responseBody),
    };
  } catch (error: any) {
    console.log(JSON.stringify(error));
    return {
      statusCode: 500,
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify({ error }),
    };
  }
};

async function getMovieCastData(movieId: number) {
  const command = new QueryCommand({
    TableName: process.env.MOVIE_CAST_TABLE, 
    KeyConditionExpression: "movieId = :m",
    ExpressionAttributeValues: {
      ":m": movieId,
    },
  });

  const { Items } = await ddbDocClient.send(command);
  return Items || [];
}

function createDDbDocClient() {
  const ddbClient = new DynamoDBClient({ region: process.env.REGION });
  const marshallOptions = {
    convertEmptyValues: true,
    removeUndefinedValues: true,
    convertClassInstanceToMap: true,
  };
  const unmarshallOptions = {
    wrapNumbers: false,
  };
  const translateConfig = { marshallOptions, unmarshallOptions };
  return DynamoDBDocumentClient.from(ddbClient, translateConfig);
}
