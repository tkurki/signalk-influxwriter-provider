/*
 * Copyright 2016 Teppo Kurki <teppo.kurki@iki.fi>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


const Transform = require('stream').Transform

const Influx = require('influx')
const camelCase = require('camelcase')
const Bacon = require('baconjs')
const Geohash = require('latlon-geohash')

const debug = require('debug')('signalk-server:influxwriter')

function InfluxWriter(options) {
  Transform.call(this, {
    objectMode: true
  });
  this.options = options;
  this.influx = new Influx.InfluxDB(options.influxurl)
  this.points = []
  this.selfContext = "vessels." + options.selfId
  this.trueWindSourceData = {
    'environment.wind.angleApparent': new Bacon.Bus(),
    'environment.wind.speedApparent': new Bacon.Bus(),
    'navigation.speedOverGround': new Bacon.Bus(),
    'navigation.courseOverGroundTrue': new Bacon.Bus()
  }
  const that = this
  Bacon.combineWith(function(awa, aws, sog, cog) {
    const points = {
      measurement: 'signalk',
      fields: getTrueWind(sog, aws, awa, cog),
      timestamp: that.timestamp
    }
    // console.log("awa:" + awa)
    // console.log("aws:" + aws)
    // console.log("sog:" + sog)
    // console.log("cog:" + cog)
    // console.log(points.fields)
    return points
  }, [
    this.trueWindSourceData['environment.wind.angleApparent'],
    this.trueWindSourceData['environment.wind.speedApparent'],
    this.trueWindSourceData['navigation.speedOverGround'],
    this.trueWindSourceData['navigation.courseOverGroundTrue']
  ]).skipDuplicates().onValue(points => {
    that.points.push(points)
  })
}

require('util').inherits(InfluxWriter, Transform);


InfluxWriter.prototype._transform = function(delta, encoding, done) {
  handleDelta(delta, this.points, this.selfContext, this.trueWindSourceData, this)
  if(this.points.length > 100) {
    this.influx.writePoints(this.points).then(() => done()).catch(err => {
      console.error("InfluxDb error: " + err.message)
      done()
    })
    this.points = []
  } else {
    done()
  }
}

InfluxWriter.prototype.end = function(callback) {
  console.log("END")
}


function getFields(pathValue) {
  if(pathValue.path === 'navigation.position') {
    const result = {}
    result[camelCase(pathValue.path)] = Geohash.encode(pathValue.value.latitude, pathValue.value.longitude)
    return result
  }
  if(typeof pathValue.value === 'number' && !isNaN(pathValue.value)) {
    const result = {}
    result[camelCase(pathValue.path)] = pathValue.value
    return result
  }
}

function handleDelta(delta, accumulator, selfContext, trueWindSources, timestampHolder) {
  // console.log(delta)
  if(delta.updates && (delta.context === selfContext || typeof delta.context === 'undefined')) {
    delta.updates.forEach(update => {
      if(update.values) {
        timestampHolder.timestamp = new Date(update.timestamp) ||  new Date()
        update.values.forEach(pathValue => {
          if(trueWindSources[pathValue.path]) {
            trueWindSources[pathValue.path].push(pathValue.value)
          }
          var fields = getFields(pathValue)
          if(fields) {
            accumulator.push({
              measurement: 'signalk',
              fields: fields,
              timestamp: timestampHolder.timestamp
            })
          }
        })
      }
    })
  }
}

function getTrueWind(speed, windSpeed, windAngle, cog) {
  var apparentX = Math.cos(windAngle) * windSpeed
  var apparentY = Math.sin(windAngle) * windSpeed
  return {
    environmentWindDirectionTrue: (Math.atan2(apparentY/(-speed + apparentX)) + cog) % (2 * Math.PI),
    environmentWindSpeedTrue: Math.sqrt(Math.pow(apparentY, 2) + Math.pow(-speed + apparentX, 2))
  }
}


module.exports = InfluxWriter;
