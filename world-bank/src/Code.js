var cc = DataStudioApp.createCommunityConnector();

function getAuthType() {
  return cc
    .newAuthTypeResponse()
    .setAuthType(cc.AuthType.NONE)
    .build();
}

function getConfig(request) {
  return {
    configParams: [],
  };
}

function getFields() {
  var fields = cc.getFields();
  var types = cc.FieldType;
  var aggregations = cc.AggregationType;

  fields
    .newDimension()
    .setId('country_name')
    .setName('Country')
    .setType(types.TEXT);

  fields
    .newDimension()
    .setId('indicator_name')
    .setName('Indicator')
    .setType(types.TEXT);

  fields
    .newDimension()
    .setId('year')
    .setName('Year')
    .setType(types.YEAR);

  fields
    .newMetric()
    .setId('value')
    .setName('Value')
    .setType(types.NUMBER)
    .setIsReaggregatable(true)
    .setAggregation(aggregations.SUM);

  return fields;
}

function getSchema(request) {
  return {
    schema: getFields().build(),
  };
}

var SERVICE_ACCOUNT_CREDS = 'SERVICE_ACCOUNT_CREDS';
var SERVICE_ACCOUNT_KEY = 'private_key';
var SERVICE_ACCOUNT_EMAIL = 'client_email';
var BILLING_PROJECT_ID = 'project_id';

/**
 * In order for this to work, you need to copy the entire credentials JSON file
 * you get from creating a service account in GCP.
 */
function getServiceAccountCreds() {
  return JSON.parse(
    PropertiesService.getScriptProperties().getProperty(SERVICE_ACCOUNT_CREDS)
  );
}

function getOauthService() {
  var serviceAccountCreds = getServiceAccountCreds();
  var serviceAccountKey = serviceAccountCreds[SERVICE_ACCOUNT_KEY];
  var serviceAccountEmail = serviceAccountCreds[SERVICE_ACCOUNT_EMAIL];

  return OAuth2.createService('WorldBankHealthPopulation')
    .setAuthorizationBaseUrl('https://accounts.google.com/o/oauth2/auth')
    .setTokenUrl('https://accounts.google.com/o/oauth2/token')
    .setPrivateKey(serviceAccountKey)
    .setIssuer(serviceAccountEmail)
    .setPropertyStore(PropertiesService.getScriptProperties())
    .setCache(CacheService.getScriptCache())
    .setScope([
      'https://www.googleapis.com/auth/bigquery',
      'https://www.googleapis.com/auth/bigquery.readonly',
    ]);
}
var sqlString =
  '\
SELECT \
  country_name \
  , indicator_name \
  , year \
  , value \
FROM \
  `bigquery-public-data.world_bank_health_population.health_nutrition_population` \
';

function getData(request) {
  var accessToken = getOauthService().getAccessToken();
  var serviceAccountCreds = getServiceAccountCreds();
  var billingProjectId = serviceAccountCreds[BILLING_PROJECT_ID];

  return DataStudioApp.createCommunityConnector()
    .newBigQueryConfig()
    .setAccessToken(accessToken)
    .setBillingProjectId(billingProjectId)
    .setUseStandardSql(true)
    .setQuery(sqlString)
    .build();
}
