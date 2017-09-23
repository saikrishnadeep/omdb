const fs = require('fs');
const Promise = require('bluebird')
const cloudscraper = require('cloudscraper');
const readline = require('readline');
const dir = './dump'

if (!fs.existsSync(dir)){
    fs.mkdirSync(dir);
}

const logger  = fs.createWriteStream('./dump/done.csv', {flags: 'a'})
const dumps = fs.createWriteStream('./dump/data.json', {flags: 'a'})
const lineReader1 = readline.createInterface({input: fs.createReadStream('./dump/done.csv')})


const debug = process.env.DEBUG
const regex = new RegExp(/tt\d{7}/, 'g')

const imdbidsDone = []
lineReader1.on('line', function (line) {
  if (regex.test(line)){
    imdbidsDone.push(line.match(regex)[0])
  }
})

const imdbids = []
lineReader1.on('close', () => {
  debug && console.log('Processed so far:', imdbidsDone.length);
  processIDs()

})


function processIDs(){
  const lineReader2 = readline.createInterface({input: fs.createReadStream('imdbids.csv')})
  lineReader2.on('line', function (line) {
    if (regex.test(line)){
      const imdbid = line.match(regex)[0]
      if (imdbidsDone.indexOf(imdbid) === -1){
        imdbids.push(imdbid)
      }
    }
  })
  lineReader2.on('close', ()=> {
    debug && console.log('Left to process:', imdbids.length);
    Promise.map(imdbids, function(imdbid, i){
      return new Promise((resolve, reject) => {
        cloudscraper.get(`http://omdbapi.com/?i=${imdbid}&apikey=57d13b99&tomatoes=true&plot=full`, function(error, response, body) {
          if (error) {
            debug && console.log('Error occurred', error);
            resolve()
            return
          }

          try {
            if (!JSON.parse(body)['Title']){
              debug && console.log('OMDB Error:',` at ${imdbid} with body ${body}`)
              resolve()
              return
            }
          } catch (e) {
            debug && console.log('Unable to parse JSON:', `at ${imdbid}`)
            resolve()
            return
          }
          
          logger.write(imdbid + '\n')
          dumps.write(body + '\n')
          resolve()
          return
        })
      })
    }, {concurrency: 30})
    .then(() => {
      debug && console.log('All done!');
    })
    .catch((err) => {
      debug && console.log(err);
    })
  })
}
