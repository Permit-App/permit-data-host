const fs = require('fs');

const readJsonFile = function (filePath) {
  const fileContents = fs.readFileSync(filePath, "utf8");
  return JSON.parse(fileContents);
}

const writeJsonFile = function (filePath, obj) {
  const jsonString = JSON.stringify(obj, null, 2);
  fs.writeFileSync(filePath, jsonString, "utf8");
}

exports.readJsonFile = readJsonFile;
exports.writeJsonFile = writeJsonFile;