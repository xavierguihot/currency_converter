
# CurrencyConverter [![Build Status](https://travis-ci.org/xavierguihot/currency_converter.svg?branch=master)](https://travis-ci.org/xavierguihot/currency_converter) [![Coverage Status](https://coveralls.io/repos/github/xavierguihot/currency_converter/badge.svg?branch=master)](https://coveralls.io/github/xavierguihot/currency_converter?branch=master) [![Release](https://jitpack.io/v/xavierguihot/currency_converter.svg)](https://jitpack.io/#xavierguihot/currency_converter)


## Overview


Version: 2.0.0

API Scaladoc: [CurrencyConverter](http://xavierguihot.com/currency_converter/#com.currency_converter.CurrencyConverter)

Scala Wrapper around your exchange rate data for currency conversion.

Based on your exchange rate files stored either on a classic file system or on
HDFS (Hadoop), this CurrencyConverter object provides for both classic and Spark
jobs tools to convert prices and retrieve exchange rates.

This doesn't provide any data. This only provides a wrapper on your feed of
exchange rates.

Compatible with Spark 2.


## Using currency_converter:


Usually, one will use the CurrencyConverter this way:

```scala
import com.currency_converter.CurrencyConverter
val currencyConverter = new CurrencyConverter("/path/to/folder/of/rate/files")
// Or when data is stored on Hadoop:
val currencyConverter = new CurrencyConverter("/hdfs/path/to/folder/of/rate/files", sparkContext)
// And then, to get the exchange rate and the converted price from EUR to SEK for the date 20170201:
currencyConverter.exchangeRate("EUR", "SEK", "20170201")
currencyConverter.convert(12.5d, "EUR", "USD", "20170201")
```

It's often the case that one doesn't need to have the exact exchange rate of the
requested date if the rate isn't available for this date. In this case, one case
use the fallback option in order to fallback on the rate of previous dates when
it's not available for the given date:

```scala
// if:
assert(currencyConverter.exchangeRate("USD", "GBP", "20170228").isFailure)
assert(currencyConverter.exchangeRate("USD", "GBP", "20170227").isFailure)
assert(currencyConverter.exchangeRate("USD", "GBP", "20170226") == Success(0.93d))
// then:
assert(currencyConverter.exchangeRate("USD", "GBP", "20170228", fallback = true) == Success(0.93d))
assert(currencyConverter.convert(2d, "USD", "GBP", "20170228", fallback = true) == Success(1.59d))
```

To load exchange rate data, this tool expects your exchange rate data to be csv
formatted this way:

	yyyyMMddDateOfApplicability,fromCurrency,toCurrency,rate (20170327,USD,EUR,0.89)

But if it's not the case, you can provide a custom exchange rate line parser,
such as:

```scala
import com.currency_converter.model.ExchangeRate
// For instance, for a custom format such as: 2017-02-01,USD,,EUR,,,0.93178,
// you could use this kind of parser:
val customRateLineParser = (rawRateLine: String) => rawRateLine.split("\\,", -1) match {

  case Array(date, fromCurrency, toCurrency, exchangeRate) => for {
    exchangeRate <- Try(exchangeRate.toDouble).toOption
    yyyyMMddDate <- Try(DateTimeFormat
      .forPattern("yyyyMMdd")
      .print(DateTimeFormat.forPattern("yyyy-MM-dd").parseDateTime(date))).toOption
  } yield ExchangeRate(yyyyMMddDate, fromCurrency, toCurrency, exchangeRate)

  case _ => None
}
```

Finally, you can request a specific range of dates for the rates to load.
Indeed, the default dates to load are 20140101 to today. This might be either
too restrictive or you might want to load less data due to very limited
available memory.


## Including currency_converter to your dependencies:


With sbt, add these lines to your build.sbt:

```scala
resolvers += "jitpack" at "https://jitpack.io"

libraryDependencies += "com.github.xavierguihot" % "currency_converter" % "v2.0.0"
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
	<version>v2.0.0</version>
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
	compile 'com.github.xavierguihot:currency_converter:v2.0.0'
}
```
