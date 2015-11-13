(function() {
  var AWS, DynamoDB, _, datify, hostname, util, uuid, winston,
    indexOf = [].indexOf || function(item) { for (var i = 0, l = this.length; i < l; i++) { if (i in this && this[i] === item) return i; } return -1; };

  winston = require("winston");

  util = require("util");

  AWS = require("aws-sdk");

  uuid = require("node-uuid");

  _ = require("lodash");

  hostname = require("os").hostname();

  datify = function(timestamp) {
    var date, i, key, keys, len;
    date = new Date(timestamp);
    date = {
      year: date.getFullYear(),
      month: date.getMonth() + 1,
      day: date.getDate(),
      hour: date.getHours(),
      minute: date.getMinutes(),
      second: date.getSeconds(),
      millisecond: date.getMilliseconds()
    };
    keys = _.without(Object.keys(date, "year", "month", "day"));
    for (i = 0, len = keys.length; i < len; i++) {
      key = keys[i];
      if (date[key] < 10) {
        date[key] = "0" + date[key];
      }
    }
    return date.year + "-" + date.month + "-" + date.day + " " + date.hour + ":" + date.minute + ":" + date.second + "." + date.millisecond;
  };

  DynamoDB = exports.DynamoDB = function(options) {
    var ref, regions;
    if (options == null) {
      options = {};
    }
    regions = ["us-east-1", "us-west-1", "us-west-2", "eu-west-1", "ap-northeast-1", "ap-southeast-1", "ap-southeast-2", "sa-east-1"];
    if (options.useEnvironment) {
      options.accessKeyId = process.env.AWS_ACCESS_KEY_ID;
      options.secretAccessKey = process.env.AWS_SECRET_ACCESS_KEY;
      options.region = process.env.AWS_REGION;
    }
    if (options.accessKeyId == null) {
      throw new Error("need accessKeyId");
    }
    if (options.secretAccessKey == null) {
      throw new Error("need secretAccessKey");
    }
    if (options.region == null) {
      throw new Error("need region");
    }
    if (ref = options.region, indexOf.call(regions, ref) < 0) {
      throw new Error("unavailable region given");
    }
    if (options.tableName == null) {
      throw new Error("need tableName");
    }
    if (!options.useEnvironment) {
      AWS.config.update({
        accessKeyId: options.accessKeyId,
        secretAccessKey: options.secretAccessKey,
        region: options.region
      });
    }

    // List of filter functions that process the message and return a 
    // different table name than the default
    this.table_filters = options.table_filters ? options.table_filters: [];

    // List of filter functions that, if matched, the log message will be
    // ignored
    this.ignore_filters = options.ignore_filters ? options.ignore_filters: [];

    this.json_ts_key = options.json_ts_key;

    this.name = "dynamodb";
    this.level = options.level || "info";
    this.db = new AWS.DynamoDB();
    this.region = options.region;
    return this.tableName = options.tableName;
  };

  util.inherits(DynamoDB, winston.Transport);

  DynamoDB.prototype.log = function(level, msg, meta, callback) {
    var table_name = this.tableName;
    var timestamp = datify(Date.now());

    // Wrap this to catch any JSON parsing exceptions. If an exception is 
    // thrown in a filter, just log the message normally
    try {

      // Skip log if matching ignore filter
      for (var i = 0; i < this.ignore_filters.length; i++){
        if (this.ignore_filters[i](msg)){
          return callback(null, 'skipped');
        }
      }

      // Run all filter functions and change table name to first matching
      for (var i = 0; i < this.table_filters.length; i++){
        var filter = this.table_filters[i];

        if (typeof(filter) == 'function'){
          var rv = filter(msg);

          if (typeof(rv) == 'string' && rv.length){
            table_name = rv;
            break;
          }
        }
      }

      // Extract timestamp from JSON, if specified
      if (this.json_ts_key){
        var data = JSON.parse(msg);
        timestamp = datify(Date.parse(data[this.json_ts_key]));
      }
    }
    catch (e){
    }

    var params;
    params = {
      TableName: table_name,
      Item: {
        id: {
          "S": uuid.v4()
        },
        level: {
          "S": level
        },
        timestamp: {
          "S": timestamp,
        },
        msg: {
          "S": msg
        },
        hostname: {
          "S": hostname
        }
      }
    };
    if (!_.isEmpty(meta)) {
      if (meta != null) {
        params.Item.meta = {
          "S": JSON.stringify(meta)
        };
      }
    }
    return this.db.putItem(params, (function(_this) {
      return function(err, data) {
        if (err) {
          _this.emit("error", err);
          if (callback) {
            callback(err, null);
          }
        } else {
          _this.emit("logged");
          if (callback) {
            callback(null, "logged");
          }
        }
      };
    })(this));
  };

  winston.transports.DynamoDB = DynamoDB;

}).call(this);
