fs = require('fs');
var lazy = require("lazy")
var mongoose = require('mongoose');
var Schema = mongoose.Schema,
  ObjectId = Schema.ObjectId;
mongoose.connect('mongodb://ygalbel:bell1234@ds027863-a0.mongolab.com:27863,ds027863-a1.mongolab.com:27863/viva1_0');

var db = mongoose.connection;
db.on('error', console.error.bind(console, 'connection error:'));

db.once('open', function(callback) {
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

  i = 0;
  dup = 0;



  new lazy(fs.createReadStream('c:\\node\\dupCats.csv'))
    .lines
    .forEach(function(line) {

      //console.log(splitted);
      var name = line.toString().split(',')[0];

   //  console.log('search ' + name);

      Cats.find({
          'EnglishLabel': name
        }, function(err, cats) {

          if (cats.length > 1) {
            console.log(cats[1].EnglishLabel);
            Cats.remove({
              '_id': mongoose.Types.ObjectId(cats[1]._id)
            }, function(err, doc) {

              //sconsole.log(err)
                //doc.EnglishLabel = newVal;
                //doc.save()
            });

          }});

      

      //console.log(old, newVal)



    });
  /*console.log(doc);
   */
});
console.log("start");