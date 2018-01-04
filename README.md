
# CurrencyConverter [![Build Status](https://travis-ci.org/xavierguihot/currency_converter.svg?branch=master)](https://travis-ci.org/xavierguihot/currency_converter) [![Coverage Status](https://coveralls.io/repos/github/xavierguihot/currency_converter/badge.svg?branch=master)](https://coveralls.io/github/xavierguihot/currency_converter?branch=master) [![Release](https://jitpack.io/v/xavierguihot/currency_converter.svg)](https://jitpack.io/#xavierguihot/currency_converter)


## Overview


Version: 1.0.4

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


With sbt, add these lines to your build.sbt:

```scala
resolvers += "jitpack" at "https://jitpack.io"

libraryDependencies += "com.github.xavierguihot" % "currency_converter" % "v1.0.4"
```

With maven, add these lines to your pom.xml:

```xml
<repositories>
	<repository>
		<id>jitpack.io</id>
		<url>https://jitpack.io</url>
	</repository>
</repositories>

<dependency>
	<groupId>com.github.xavierguihot</groupId>
	<artifactId>currency_converter</artifactId>
	<version>v1.0.4</version>
</dependency>
```

With gradle, add these lines to your build.gradle:

```groovy
allprojects {
	repositories {
		maven { url 'https://jitpack.io' }
	}
}

dependencies {
	compile 'com.github.xavierguihot:currency_converter:v1.0.4'
}
```
