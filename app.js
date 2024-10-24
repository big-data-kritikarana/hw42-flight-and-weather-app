'use strict';
const http = require('http');
var assert = require('assert');
const express= require('express');
const app = express();
const mustache = require('mustache');
const filesystem = require('fs');
require('dotenv').config()
const port = Number(process.argv[2]);
const hbase = require('hbase')

const url = new URL(process.argv[3]);

var hclient = hbase({
	host: url.hostname,
	path: url.pathname ?? "/",
	port: url.port ?? 'http' ? 80 : 443, // http or https defaults
	protocol: url.protocol.slice(0, -1), // Don't want the colon
	encoding: 'latin1',
	auth: process.env.HBASE_AUTH
});

function counterToNumber(c) {
	return Number(Buffer.from(c, 'latin1').readBigInt64BE());
}
function rowToMap(row) {
	if (!row) {
		console.error("No data returned for the specified row.");
		return {};
	}

	var stats = {}
	row.forEach(function (item) {
		stats[item['column']] = counterToNumber(item['$'])
	});
	return stats;
}

app.use(express.static('public'));
app.get('/delays.html',function (req, res) {
    const origin=req.query['origin'];
	const year = req.query['year'];
	const rowKey = `${origin}_${year}`;
    console.log(rowKey);
	hclient.table('kritikarana_hw42_weather_delays_by_origin_year_hbase').row(rowKey).get(function (err, cells) {
		if (err) {
			console.error("Error fetching row data:", err);
		}
		const weatherInfo = rowToMap(cells);
		console.log(weatherInfo)

		function avg_weather_delay(weather) {
			var flights = weatherInfo["delay:" + weather + "_flights"];
			var delays = weatherInfo["delay:" + weather + "_delays"];
			if(flights == 0)
				return " - ";
			return (delays/flights).toFixed(1); /* One decimal place */
		}

		function total_weather_delay(weather) {
			var delays = weatherInfo["delay:" + weather + "_delays"];
			return (delays); /* One decimal place */
		}

		var template = filesystem.readFileSync("result.mustache").toString();
		var html = mustache.render(template,  {
			origin : req.query['origin'],
			year : req.query['year'],

			// Average Delays
			avg_clear_dly: avg_weather_delay("clear"),
			avg_fog_dly: avg_weather_delay("fog"),
			avg_rain_dly: avg_weather_delay("rain"),
			avg_snow_dly: avg_weather_delay("snow"),
			avg_hail_dly: avg_weather_delay("hail"),
			avg_thunder_dly: avg_weather_delay("thunder"),
			avg_tornado_dly: avg_weather_delay("tornado"),

			// Total Delays
			total_clear_dly: total_weather_delay("clear"),
			total_fog_dly: total_weather_delay("fog"),
			total_rain_dly: total_weather_delay("rain"),
			total_snow_dly: total_weather_delay("snow"),
			total_hail_dly: total_weather_delay("hail"),
			total_thunder_dly: total_weather_delay("thunder"),
			total_tornado_dly: total_weather_delay("tornado")
		});
		res.send(html);
	});
});


app.listen(port);
