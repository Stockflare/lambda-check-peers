// dependencies
var AWS = require('aws-sdk');
var _ = require('underscore');
var _l = require('lodash');
var path = require('path');
var when = require('when');
var moment = require('moment');
var rest = require('restler');

exports.handler = function(event, context) {

  console.log('event');
  console.log(event);

  var region;
  if (event.region) {
    region = event.region;
  } else {
    context.succeed("region is not set");
  }


  var search_url;
  if (event.search_url) {
    search_url = event.search_url;
  } else {
    context.succeed("search_url is not set");
  }

  var peers_url;
  if (event.peers_url) {
    peers_url = event.peers_url;
  } else {
    context.succeed("peers_url is not set");
  }

  var total_pages = 99999;

  var max_pages = 99999;

  if (!_.isUndefined(event.max)) {
    max_pages = event.max;
  }

  // Get all stocks that match the search query
  when.unfold(function(page){
    // unspool
    // Get page of stocks
    return when.promise(function(resolve, reject, notify){
      var search_body = _.extend(event.search_body, {page: page, select: ["sic", "ric", "short_name"]});
      rest.putJson(event.search_url, search_body)
      .on('success', function(data, response){
        console.log('Got page: ' + page);
        total_pages = parseInt(response.headers['x-api-pages']);
        resolve([data, page + 1]);
      }).on('fail', function(data, response){
        console.log('Error:', data, response);
        reject(data);
      });
    });

  }, function(page){
    // predicate
    return page >= total_pages || page > max_pages;
  }, function(data){
    // handler

    // Have a page of stocks, check the peer list for each one
    return when.iterate(function(index){
      // f
      return index + 1;
    }, function(index){
      // predicate
      return index >= data.length;
    }, function(index){
      // handler
      // Check a Stock
      var stock = data[index];
      return when.promise(function(resolve, reject, notify){
        var params = {
          sic: stock.sic
        };
        var call = function(){
          rest.get(peers_url + "?sic=" + stock.sic)
          .on('success', function(data, response){
            if (data.length === 0) {
              console.log("Zero peers for:");
              console.log(stock);
              console.log(data);
            } else {
              console.log('Checked: ' + stock.sic);
            }
            resolve();
          })
          .on('fail', function(data, response){
            console.log('Error getting peers');
            call();
          });
        };
        call();

      });
    }, 0);
  }, 1)
  .done(function(){
    console.log("Successfully checked all stocks");
    context.succeed("Successfully checked all stocks");
  }, function(reason){
    console.log("Failed to check all stocks: " + reason);
    context.fail("Failed to check all stocks: " + reason);
  });

};
