var BigQuery = require('@google-cloud/bigquery');
var Promise = require('bluebird');

var executeQuery = function (bigQueryConfig, bigQueryConfigPath, user, startTime, endTime) {
  var bigquery = BigQuery({
    projectId: bigQueryConfig.project_id,
    keyFilename: bigQueryConfigPath
  });

  startTime = startTime || '2016-01-01';
  endTime = endTime || '2016-01-01';

  var QUERY_STRING = `SELECT repo_name FROM( SELECT type, JSON_EXTRACT(payload, '$.action') as event, JSON_EXTRACT(payload, '$.pull_request.merged') as merged, JSON_EXTRACT(payload, '$.pull_request.user.login') as sender, JSON_EXTRACT(payload, '$.pull_request.base.repo.owner.login') as owner, JSON_EXTRACT(payload, '$.pull_request.base.repo.full_name') as repo_name, FROM (TABLE_DATE_RANGE([githubarchive:day.], TIMESTAMP('${startTime}'), TIMESTAMP('${endTime}'))) WHERE type = 'PullRequestEvent' ) WHERE event = '"closed"' AND merged = 'true' AND sender = '"${user}"' AND owner != '"${user}"' Group by repo_name`;

  return new Promise(function(resolve, reject) {
    bigquery.query(QUERY_STRING, function (err, rows) {
      if (err) {
        reject(err);
        return;
      }
      resolve(rows);
    });
  });
};

module.exports = function (configs, callback) {
  var bigQueryConfig;
  if (!configs.bigQueryConfigPath || !configs.user) {
    return false;
  }
  try {
    bigQueryConfig = require(configs.bigQueryConfigPath);
  } catch (e) {
    console.error('[bluering-v2] parse bigQueryConfigPath fail');
    return false;
  }

  return executeQuery(
    bigQueryConfig,
    configs.bigQueryConfigPath,
    configs.user,
    configs.startTime,
    configs.endTime);
};
