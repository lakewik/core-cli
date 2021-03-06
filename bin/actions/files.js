'use strict';
var log = require('./../logger')().log;
var utils = require('./../utils');
var storj = require('storj-lib');

module.exports.list = function(bucketid) {
  var client = this._storj.PrivateClient();
  bucketid = this._storj.getRealBucketId(bucketid);

  client.listFilesInBucket(bucketid, function(err, files) {
    if (err) {
      return log('error', err.message);
    }

    if (!files.length) {
      return log('warn', 'There are no files in this bucket.');
    }

    files.forEach(function(file) {
      log(
        'info',
        'Name: %s, Type: %s, Size: %s bytes, ID: %s',
        [file.filename, file.mimetype, file.size, file.id]
      );
    });
  });
};

module.exports.getInfo = function(bucketid, fileid) {
   var client = this._storj.PrivateClient();
   bucketid = this._storj.getRealBucketId(bucketid);
   fileid = this._storj.getRealFileId(bucketid, fileid);

  client.getFileInfo(bucketid, fileid, function(err, file) {
     if (err) {
       return log('error', err.message);
     }

    log(
      'info',
      'Name: %s, Type: %s, Size: %s bytes, ID: %s',
      [file.filename, file.mimetype, file.size, file.id]
    );
  });
};

module.exports.remove = function(id, fileId, env) {
  var client = this._storj.PrivateClient();
  var keypass = this._storj.getKeyPass();
  id = this._storj.getRealBucketId(id);
  fileId = this._storj.getRealFileId(id, fileId);

  function destroyFile() {
    utils.getKeyRing(keypass, function(keyring) {
      client.removeFileFromBucket(id, fileId, function(err) {
        if (err) {
          return log('error', err.message);
        }

        log('info', 'File was successfully removed from bucket.');
        keyring.del(fileId);
      });
    });
  }

  if (!env.force) {
    return utils.getConfirmation(
      'Are you sure you want to destroy the file?',
      destroyFile
    );
  }

  destroyFile();
};

module.exports.mirror = function(bucket, file, env) {
  var client = this._storj.PrivateClient({ requestTimeout: 30000 });
  bucket = this._storj.getRealBucketId(bucket);
  file = this._storj.getRealFileId(bucket, file);

  if (parseInt(env.redundancy) > 12 || parseInt(env.redundancy) < 1) {
    return log('error', '%s is an invalid Redundancy value.', env.redundancy);
  }

  log(
    'info',
    'Establishing %s mirrors per shard for redundancy',
    [env.redundancy]
  );
  client.replicateFileFromBucket(
    bucket,
    file,
    parseInt(env.redundancy),
    function(err, replicas) {
      if (err) {
        return log('error', err.message);
      }

      replicas.forEach(function(shard, i) {
        log('info', 'Shard %s establishing mirrors to %s nodes', [
          i,
          shard.length
        ]);
      });

      process.exit();
    }
  );
};

module.exports.stream = function(bucket, id, env) {
  var self = this;
  var client = this._storj.PrivateClient({
    logger: storj.deps.kad.Logger(0)
  });
  var keypass = this._storj.getKeyPass();
  bucket = this._storj.getRealBucketId(bucket);
  id = this._storj.getRealFileId(bucket, id);

  utils.getKeyRing(keypass, function(keyring) {
    var secret = keyring.get(id);

    if (!secret) {
      return log('error', 'No decryption key found in key ring!');
    }

    var decrypter = new storj.DecryptStream(secret);
    var exclude = env.exclude.split(',');

    client.createFileStream(bucket, id, function(err, stream) {
      if (err) {
        return process.stderr.write(err.message);
      }

      stream.on('error', function(err) {
        log('warn', 'Failed to download shard, reason: %s', [err.message]);

        if (!err.pointer) {
          return;
        }

        log('info', 'Retrying download from other mirrors...');
        exclude.push(err.pointer.farmer.nodeID);
        module.exports.stream.call(
          self,
          bucket,
          id,
          { exclude: env.exclude.join(',') }
        );
      }).pipe(decrypter).pipe(process.stdout);
    });
  });
};

module.exports.getpointers = function(bucket, id, env) {
  var client = this._storj.PrivateClient();
  bucket = this._storj.getRealBucketId(bucket);
  id = this._storj.getRealFileId(bucket, id);

  client.createToken(bucket, 'PULL', function(err, token) {
    if (err) {
      return log('error', err.message);
    }

    var skip = Number(env.skip);
    var limit = Number(env.limit);

    client.getFilePointers({
      bucket: bucket,
      file: id,
      token: token.token,
      skip: skip,
      limit: limit
    }, function(err, pointers) {
      if (err) {
        return log('error', err.message);
      }

      if (!pointers.length) {
        return log('warn', 'There are no pointers to return for that range');
      }

      log('info', 'Listing pointers for shards %s - %s', [
        skip, skip + pointers.length - 1
      ]);
      log('info', '-----------------------------------------');
      log('info', '');
      pointers.forEach(function(location, i) {
        log('info', 'Index:  %s', [skip + i]);
        log('info', 'Hash:   %s', [location.hash]);
        log('info', 'Token:  %s', [location.token]);
        log('info', 'Farmer: %s', [
          storj.utils.getContactURL(location.farmer)
        ]);
        log('info', '');
      });
    });
  });
};
