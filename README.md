
# CurrencyConverter [![Build Status](https://travis-ci.org/XavierGuihot/currency_converter.svg?branch=master)](https://travis-ci.org/XavierGuihot/currency_converter) [![Coverage Status](https://coveralls.io/repos/github/XavierGuihot/currency_converter/badge.svg?branch=master)](https://coveralls.io/github/XavierGuihot/currency_converter?branch=master)


## Overview


Version: 1.0.3

API Scaladoc: [CurrencyConverter](http://xavierguihot.com/currency_converter/#com.currency_converter.CurrencyConverter)

Scala Wrapper around your exchange rate data for currency conversion.

Based on your exchange rate files stored either on a classic file system or on
HDFS (Hadoop), this CurrencyConverter object provides for both classic and Spark
jobs methods to convert prices and retrieve exchange rates.

This doesn't provide any data. This only provides a wrapper on your feed of
exchange rates.

Compatible with Spark 2.


## Using currency_converter:


Different exemples describing how to create the CurrencyConverter and how to use 
it are available in the [Scaladoc](http://xavierguihot.com/currency_converter/#com.currency_converter.CurrencyConverter).

Here is one of the many ways to use the currency converter (very basic use case):

```scala
import com.currency_converter.CurrencyConverter

val currencyConverter = new CurrencyConverter(
	"path/to/folder/of/rate/files"
)
// Or in order to use it with Spark:
val currencyConverter = new CurrencyConverter(
	"/hdfs/path/to/folder/of/rate/files", Some(sparkContext)
)

assert(currencyConverter.getExchangeRate("EUR", "SEK", "20170201") == 9.444644d)
assert(currencyConverter.convert(12.5d, "EUR", "USD", "20170201") == 13.415185d)

// In case you want to fallback on previous dates when the rate is not available for the date you're requesting:
assert(currencyConverter.getExchangeRateAndFallback("USD", "GBP", "20170228") == 0.9317799d)
```

The full list of methods is available at [CurrencyConverter](http://xavierguihot.com/currency_converter/#com.currency_converter.CurrencyConverter).


## Including currency_converter to your dependencies:


With sbt, just add this one line to your build.sbt:

	libraryDependencies += "currency_converter" % "currency_converter" % "1.0.3" from "https://github.com/xavierguihot/currency_converter/releases/download/v1.0.3/currency_converter-1.0.3.jar"
