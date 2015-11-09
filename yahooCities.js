var sleep = require('sleep');
var Crawler = require("crawler");
var fs = require('fs');
var libphonenumber = require('libphonenumber')
var util = require('util');
var url = require('url');

var redis = require('redis');
var client = redis.createClient(); //creates a new client 
var prod = 0;

client.on('connect', function() {
  //console.log('connected');
  var mongoose = require('mongoose');

  var Schema = mongoose.Schema;
  ObjectId = Schema.ObjectId;
  mongoose.connect('mongodb://ygalbel:bell1234@ds027863-a0.mongolab.com:27863,ds027863-a1.mongolab.com:27863/viva1_0');

  var db = mongoose.connection;
  db.on('error', function() {
    console.log('error no Mongo connection');
    process.exit();
  });

  db.once('open', function(callback) {
    var businessSchema = mongoose.Schema({
      business_name: String,
      address: String,
      source: String,
      MainCategoryId: ObjectId,
      country: String,
      CanonizedNumber: [String]
    }, {
      versionKey: false
    });



    var prefix = process.argv[2];
    var cityName = '';
    var modelName = 'business' + prefix;

    var Business = mongoose.model(modelName, businessSchema);
    var query = Business.findOne({
      'source': cityName
    });

    var categorySchema = mongoose.Schema({
      _id: ObjectId,
      Label: String,
      EnglishLabel: String,
      CatId: String,
      IsPrivate: Boolean,
      IsSide: Boolean,
      IsInstitution: Boolean,
      IsActive: String,
      Keywords: [String]
    }, {
      collection: 'Category',
      versionKey: false
    });

    var Cats = mongoose.model('Category', categorySchema);
    var stream = Cats.find().stream();
    var dict = {};
    stream.on('data', function(doc) {

      dict[doc.EnglishLabel] = doc._id;
    });



    stream.on('close', function() {
      //console.log(dict);

      //stop after two hours
      setTimeout(function() {
      //  console.log('finish city ' + cityName + ' prematurely  found ' + prod + ' businesses');
        process.exit();
      }, 1000 * 60 * 60 * 2);


      // execute the query at a later time
      query.exec(function(err, bus) {
        if (err) return;
        if (bus !== null) {
          console.log('alredy view ' + cityName);
          process.exit();
        }
      });

    //  console.log('cityname ' + cityName);

      String.prototype.isEmpty = function() {
        return (this.length === 0 || !this.trim());
      };
      

      
      var i = 0;
    ///  console.log('connected');


for (var i = 0; i < 22; i++) {


  c.queue({
        uri: 'https://local.yahoo.com/' + prefix + '/'+ String.fromCharCode(97 + i)+'.html;_ylt=ArNG7ikf8AWhg8xy8zY9N_meNcIF;_ylv=3'
          //uri: 'https://local.yahoo.com/TX'
      });
};

      
    });

var isFirstDrain = true;
    var c = new Crawler({
        //  rateLimits: 1,
        maxConnections: 10,
        //  skipDuplicates: true,
        userAgent: 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36',
        onDrain: function() {

          if (!isFirstDrain) {

          }
        },
        // This will be called for each crawled page
        callback: function(error, result, $) {

          //console.log('here');
          // $ is Cheerio by default
          //a lean implementation of core jQuery designed specifically for the server
          //fs.appendFile('c:\\node\\viewPageyahoo1.txt', result.request.uri.href + '\n');

          // console.log(new Date().toString(), result.request.uri.href, c.queueItemSize.toString());
          // check is city page


          if (result.body.indexOf('Cities') > 0) {
            addAllCities($, c);

          } //
          /*else if (result.body.indexOf('Browse by Category') > 0) {

            addAllCategories($, c);
          } else if (result.body.indexOf('You have refined by') > 0) {
            $('.yls-gl-pagination > a').each(function(a, b) {
              addToQ(c, alreadyView, b.attribs.href, 'PAGIN');
            });

            handleProductPage($);

          } else if (isCategoryPage($, c)) {
            // console.log("isCategoryPage", result.request.uri.href);
          } else {
            // console.log("can't recognize", result.request.uri.href);
          }
*/
          //console.log(result.statusCode);
          if (result.statusCode == 999) {
            console.log(new Date().toString().substr(16, 8), 'WAIT', result.request.uri.href, c.queueItemSize.toString());
            sleep.sleep(60 * 5); // wait 5 min and requeu
            c.queue({
              url: result.request.uri.href
            });
          }
          //throw "error";

  /*        $('a').each(function(index, a) {

            var toQueueUrl = $(a).attr('href');
            //console.log(toQueueUrl.indexOf('.html'), toQueueUrl)
            try {
              if (toQueueUrl.indexOf('https://local') == 0 && toQueueUrl.indexOf('.html') == 28) {
                addToQ(c, alreadyView, toQueueUrl, 'ENTER');
              }
            } catch (e) {
/////              //console.log("error " + e + toQueueUrl);
            }
*/


          //});

          

        }
      });

    function addToQ(c, alreadyView, url, prefix) {

      // avoid duplicate because yahoo session
      if (url.indexOf(';') > 0) {
        url = url.substr(0, url.indexOf(';'));
      }

      var validPrefix = prefix.toLowerCase();
      //console.log("addToQ " + url.toLowerCase() + " " + validPrefix + " " + url.toLowerCase().indexOf(validPrefix));
      //console.log(url.toLowerCase().indexOf('/' + prefix.toLowerCase() + '/' + cityName.toLowerCase()) > -1)
      
      if (url.toLowerCase().indexOf(validPrefix) > -1 && alreadyView.indexOf(url) == -1) {
        //console.log(new Date().toString().substr(16, 8), url);
        alreadyView.push(url);
        isFirstDrain = false;
        c.queue({
          url: url
        });
      }
    }

    alreadyView = [];

    var addAllCities = function($, c) {
      $(".yls-br-toplist > li > ul > li> a").each(function(a, b) {
         console.log('node yahooLocal.js ' + $(b).text().replace(" ", "+") +' '  + prefix + ' >> output' + prefix + '.txt');


        toQueueUrl = b.attribs.href;
        //addToQ(c, alreadyView, toQueueUrl, prefix)
      });
    }

    var addAllCategories = function($, c) {
      $("#yls-hm-browse-rt > li > a").each(function(a, b) {
        // console.log($(b).text());

        toQueueUrl = b.attribs.href;
        addToQ(c, alreadyView, toQueueUrl, prefix);
      });
    }

    var isCategoryPage = function($, c) {
      foundCat = false;
      $("#yls-br-catlist > li > a").each(function(a, b) {
        toQueueUrl = b.attribs.href;
        foundCat = true;
        addToQ(c, alreadyView, toQueueUrl, prefix);
        return foundCat;
      });
    }

    var alreadyView = [];

    var handleProductPage = function($) {

      var titles = [];
      var tel = [];
      var addr = [];
      var mainCat = '';
      var secCat = '';
      var cats = [];
      var thirdCat = $($("em > span")).text();
      //yls-gl-breadcrumb
      //console.log($('.yls-gl-breadcrumb'));
      $('.yls-gl-breadcrumb > a').each(function(a, b) {
        cats.push($(b).text());
      });

      mainCat = cats[cats.length - 2];
      secCat = cats[cats.length - 1];

      // console.log(mainCat, secCat); 
      $('.yls-rs-listing-title').each(function(a, b) {
        titles.push($(b).text());

      });


      $('.tel').each(function(a, b) {
        tel.push($(b).text());

      });


      $('.adr').each(function(a, b) {
        addr.push($(b).text().trimLeft().trimRight())
      });


      for (var i = 0; i < titles.length; i++) {
        prod++;

        libphonenumber.intl(tel[i], 'US', function(error, result) {
          //        console.log(doc.phone, result)
          var data = new Business({
            business_name: titles[i],
            address: addr[i],
            source: cityName,
            country: prefix
          });

          if (thirdCat != null && (thirdCat in dict)) {
            data.MainCategoryId = dict[thirdCat];
          } else if (secCat != null && (secCat in dict)) {
            data.MainCategoryId = dict[secCat];
          }

          data.CanonizedNumber.push(result);

          key = createKey(data);
          client.get(key, function(err, reply) {
            if (reply == null) {
              data.save(function(err, fluffy) {
                if (err) return console.error(err);
              });

              client.set(key, ' ');
            }
          });



        });


        //str = titles[i] + '\t' + tel[i] + '\t' + addr[i] + '\t' + mainCat + '\t' + secCat + '\t' + cityName + '\n';
        //fs.appendFile(cityName + '.txt', str);

      };
    }

    //https://local.yahoo.com/TX/Abbott
    console.log(new Date().toString() + " start");
  });
});

function createKey(doc) {
  return doc.business_name + doc.phone;
}