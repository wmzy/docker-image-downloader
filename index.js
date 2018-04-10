#!/usr/bin/env node

const os = require('os');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');
const commander = require('commander');
const drc = require('docker-registry-client');
const mkdir = require('make-dir');
const _ = require('lodash/fp');
const createWriteStream = require('fs-write-stream-atomic');
const tar = require('tar-stream');
const PQueue = require('p-queue');
const pkg = require('./package');
const pack = tar.pack();

commander
  .version(pkg.version)
  .option('-i, --insecure')
  .option('-o, --output <path>')
  .option('-u, --username <username>')
  .option('-p, --password <password>')
  .arguments('<repo>')
  .action(did)
  .parse(process.argv);

function did(repo, cmd) {
  const [name, ref] = repo.split(':')
  const cachePath = path.join(os.tmpdir(), 'docker-image-download-cache');

  mkdir.sync(cachePath);

  const client = drc.createClientV2({
    name: name,
    username: cmd.username,
    password: cmd.password,
    insecure: cmd.insecure
  });

  client.getManifest({
    ref: ref || 'latest'
  }, function (err, manifest) {
    console.log(err, manifest)
    if (err) process.exit(1);
    if (manifest.schemaVersion === 1) {
      return downloadImageV1(manifest);
    }
    downloadImageV2(manifest)
  })

  function downloadImageV1(manifest) {
    const pack = tar.pack();
    const queue = new PQueue({concurrency: 1})
    const imageJSONList = _.map('v1Compatibility', manifest.history);
    const getLayerId = _.pipe(JSON.parse, _.get('id'));
    const layerIdList = _.map(getLayerId, imageJSONList);
    const imageId = _.head(layerIdList);

    pack.pipe(fs.createWriteStream(cmd.output));

    _.pipe(
      _.zipAll,
      _.map(([fl, imageJSON, layerId]) => {
        pack.entry({name: `${layerId}/VERSION`}, '1.0');
        pack.entry({name: `${layerId}/json`}, imageJSON);
        return downloadLayer(fl.blobSum)
          .then(() => {
            queue.add(() => {
              const layerPath = path.join(cachePath, `${fl}.tar`);
              const fileStream = fs.createReadStream(layerPath);
              const entry = pack.entry({name: `${layerId}/layer.tar`});
              return new Promise((res, rej) => {
                fileStream
                  .on('error', rej)
                  .pipe(entry)
                  .on('error', rej)
                  .on('end', res);
              })
            })
          })
      }),
      Promise.all.bind(Promise)
    )([manifest.fsLayers, imageJSONList, layerIdList])
      .then(() => pack.finalize())
      .catch(e => {
        console.error(e);
        process.exit(1);
      });
  }

  function downloadImageV2(manifest) {
    // todo
  }

  function downloadLayer(blobSum) {
    const layerPath = path.join(cachePath, `${blobSum}.tar`);

    return new Promise((res, rej) => {
      fs.access(layerPath, err => {
        if (!err) return res()
      })
      console.log(blobSum, 'bbbbbbbbb')
      client.createBlobReadStream({
        digest: blobSum
      }, (err, s) => {
        if (err) return rej(err);
        s.on('error', rej);
        s.pipe(fs.createWriteStream(layerPath))
          .on('end', res)
          .on('error', rej);
      });
    })
  }
}
