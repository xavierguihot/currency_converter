package com.currency_converter

import com.currency_converter.error.CurrencyConverterException
import com.currency_converter.model.ExchangeRate

import scala.util.Success

import com.holdenkarau.spark.testing.SharedSparkContext

import org.scalatest.FunSuite

/** Testing facility for Currency Conversion base methods.
  *
  * @author Xavier Guihot
  * @since 2017-02
  */
class CurrencyConverterTest extends FunSuite with SharedSparkContext {

  test("Get Exchange Rate") {

    val currencyConverter =
      new CurrencyConverter(
        "src/test/resources/hdfs_rates",
        sc,
        "20170201",
        "20170201")

    assert(currencyConverter.allDatesHaveRates())

    // Direct application of what's in the rate file:
    var rate = currencyConverter.exchangeRate("USD", "EUR", "20170201")
    assert(rate === Success(0.93178d))
    rate = currencyConverter.exchangeRate("USD", "USD", "20170201")
    assert(rate === Success(1d))
    rate = currencyConverter.exchangeRate("USD", "SEK", "20170201")
    assert(rate === Success(8.80033d))
    rate = currencyConverter
      .exchangeRate("USD", "EUR", "2017-02-01", "yyyy-MM-dd")
    assert(rate === Success(0.93178d))
    rate = currencyConverter
      .exchangeRate("USD", "USD", "2017-02-01", "yyyy-MM-dd")
    assert(rate === Success(1d))
    rate = currencyConverter
      .exchangeRate("USD", "SEK", "2017-02-01", "yyyy-MM-dd")
    assert(rate === Success(8.80033d))
    rate = currencyConverter.exchangeRate("USD", "EUR", "170201", "yyMMdd")
    assert(rate === Success(0.93178d))

    // Opposite:
    assert(
      currencyConverter.exchangeRate("EUR", "USD", "20170201") === Success(
        1.0732147073343492d))
    assert(
      currencyConverter.exchangeRate("SEK", "USD", "20170201") === Success(
        0.1136321024325224d))
    assert(
      currencyConverter
        .exchangeRate("EUR", "USD", "2017-02-01", "yyyy-MM-dd") === Success(
        1.0732147073343492d))
    assert(
      currencyConverter
        .exchangeRate("SEK", "USD", "2017-02-01", "yyyy-MM-dd") === Success(
        0.1136321024325224d))

    // With something else than USD:
    assert(
      currencyConverter.exchangeRate("EUR", "SEK", "20170201") === Success(
        9.444643585395694d))
    assert(
      currencyConverter
        .exchangeRate("EUR", "SEK", "2017-02-01", "yyyy-MM-dd") === Success(
        9.444643585395694d))

    // With a non existing date:
    var exception = intercept[CurrencyConverterException] {
      currencyConverter.exchangeRate("EUR", "SEK", "20170202").get
    }
    assert(exception.getMessage === "No exchange rate for date \"20170202\".")
    exception = intercept[CurrencyConverterException] {
      currencyConverter
        .exchangeRate("EUR", "SEK", "2017-02-02", "yyyy-MM-dd")
        .get
    }
    assert(exception.getMessage === "No exchange rate for date \"20170202\".")

    // With a missing currency:
    exception = intercept[CurrencyConverterException] {
      currencyConverter.exchangeRate("...", "SEK", "20170201").get
    }
    var expectedMessage =
      "No exchange rate for currency \"...\" for date \"20170201\"."
    assert(exception.getMessage === expectedMessage)
    exception = intercept[CurrencyConverterException] {
      currencyConverter
        .exchangeRate("...", "SEK", "2017-02-01", "yyyy-MM-dd")
        .get
    }
    expectedMessage =
      "No exchange rate for currency \"...\" for date \"20170201\"."
    assert(exception.getMessage === expectedMessage)

    // With a non existing currency, but from this currency to the same
    // currency:
    assert(
      currencyConverter.exchangeRate("XXX", "XXX", "20110719") === Success(1d))
  }

  test("Get Exchange Rate after Loading with a Custom Rate Format") {

    // The custom format is:
    //     2017-02-01,USD,,EUR,,,0.93178
    // instead of the default one which would be:
    //     20170201,USD,EUR,0.93178
    // Notice how we use a function here and not a method (val instead of
    // def), because otherwise, the method is not Serializable and this will
    // throw a not Serializable exception at run time:
    val customRateLineParser = (rawRateLine: String) => {

      val splitRateLine = rawRateLine.split("\\,", -1)

      val date = splitRateLine(0).replace("-", "")
      val fromCurrency = splitRateLine(1)
      val toCurrency = splitRateLine(3)
      val exchangeRate = splitRateLine(6).toFloat

      Some(ExchangeRate(date, fromCurrency, toCurrency, exchangeRate))
    }

    val currencyConverter = new CurrencyConverter(
      "src/test/resources/hdfs_rates_custom_format",
      sc,
      "20170201",
      "20170201",
      customRateLineParser)

    assert(
      currencyConverter.exchangeRate("USD", "EUR", "20170201") === Success(
        0.9317799806594848d))
    assert(
      currencyConverter.exchangeRate("USD", "USD", "20170201") === Success(1d))
    assert(
      currencyConverter.exchangeRate("USD", "SEK", "20170201") === Success(
        8.80033016204834d))
    assert(
      currencyConverter.exchangeRate("EUR", "USD", "20170201") === Success(
        1.073214729610558d))
    assert(
      currencyConverter
        .exchangeRate("EUR", "SEK", "2017-02-01", "yyyy-MM-dd") === Success(
        9.444643955346347d))
  }

  test("Get Exchange Rate after Loading Rates from a Classic File System") {

    val currencyConverter = new CurrencyConverter(
      "src/test/resources/hdfs_rates",
      "20170201",
      "20170201")

    assert(
      currencyConverter.exchangeRate("USD", "EUR", "20170201") === Success(
        0.93178d))
    assert(
      currencyConverter.exchangeRate("USD", "USD", "20170201") === Success(1d))
    assert(
      currencyConverter.exchangeRate("USD", "SEK", "20170201") === Success(
        8.80033d))
  }

  test("Get Converted Price") {

    val currencyConverter = new CurrencyConverter(
      "src/test/resources/hdfs_rates",
      sc,
      "20170201",
      "20170201")

    assert(
      currencyConverter.convert(1d, "USD", "USD", "20170201") === Success(1d))
    assert(
      currencyConverter.convert(1d, "EUR", "USD", "20170201") === Success(
        1.0732147073343492d))
    assert(
      currencyConverter.convert(1d, "EUR", "SEK", "20170201") === Success(
        9.444643585395694d))

    assert(
      currencyConverter
        .convert(1d, "USD", "USD", "170201", "yyMMdd") === Success(1d))
    assert(
      currencyConverter
        .convert(1d, "EUR", "USD", "2017-02-01", "yyyy-MM-dd") === Success(
        1.0732147073343492d))
    assert(
      currencyConverter
        .convert(1d, "EUR", "SEK", "170201", "yyMMdd") === Success(
        9.444643585395694d))

    assert(
      currencyConverter.convert(12.5d, "USD", "USD", "20170201") === Success(
        12.5d))
    assert(
      currencyConverter.convert(12.5d, "EUR", "USD", "20170201") === Success(
        13.415183841679365d))
    assert(
      currencyConverter.convert(12.5d, "EUR", "SEK", "20170201") === Success(
        118.05804481744617d))

    // With a non existing date (this is all taken care of by getExchangeRate,
    // but let's test it anyway):
    var exception = intercept[CurrencyConverterException] {
      currencyConverter.convert(12.5d, "EUR", "SEK", "20170202").get
    }
    assert(exception.getMessage === "No exchange rate for date \"20170202\".")
    exception = intercept[CurrencyConverterException] {
      currencyConverter.convert(12.5d, "EUR", "SEK", "170202", "yyMMdd").get
    }
    assert(exception.getMessage === "No exchange rate for date \"20170202\".")
  }

  test("Get Exchange Rate with Fallback") {

    val currencyConverter = new CurrencyConverter(
      "src/test/resources/hdfs_rates_fallback",
      sc,
      "20170201",
      "20170228")

    assert(!currencyConverter.allDatesHaveRates())

    // 1: Let's try even if there's no need to fallback:
    assert(
      currencyConverter.exchangeRate("USD", "SEK", "20170228") === Success(
        8.4856d))
    var exchangeRate =
      currencyConverter.exchangeRate("USD", "SEK", "20170228", fallback = true)
    assert(exchangeRate === Success(8.4856d))
    exchangeRate = currencyConverter
      .exchangeRate("USD", "SEK", "2017-02-28", "yyyy-MM-dd", fallback = true)
    assert(exchangeRate === Success(8.4856d))

    // 2: USD to EUR rate is not available for 20170228, but is available for
    // 20170227:
    assert(currencyConverter.exchangeRate("USD", "EUR", "20170228").isFailure)
    assert(
      currencyConverter.exchangeRate("USD", "EUR", "20170227") === Success(
        1.25d))
    exchangeRate = currencyConverter
      .exchangeRate("USD", "EUR", "20170228", fallback = true)
    assert(exchangeRate === Success(1.25d))
    exchangeRate = currencyConverter
      .exchangeRate("USD", "EUR", "170228", "yyMMdd", fallback = true)
    assert(exchangeRate === Success(1.25d))

    // 3: USD to EUR rate is not available for 20170228, and not available for
    // 20170227 but is available for 20170201:
    assert(currencyConverter.exchangeRate("USD", "GBP", "20170228").isFailure)
    assert(currencyConverter.exchangeRate("USD", "GBP", "20170227").isFailure)
    exchangeRate = currencyConverter.exchangeRate("USD", "GBP", "20170201")
    assert(exchangeRate === Success(0.79919d))
    exchangeRate = currencyConverter
      .exchangeRate("USD", "GBP", "20170228", fallback = true)
    assert(exchangeRate === Success(0.79919d))
    exchangeRate = currencyConverter
      .exchangeRate("USD", "GBP", "2017-02-28", "yyyy-MM-dd", fallback = true)
    assert(exchangeRate === Success(0.79919d))

    // 4: Let's check we do get an exception if there are absolutely no dates
    // with the requested rate:
    var exception = intercept[CurrencyConverterException] {
      currencyConverter
        .exchangeRate("USD", "XXX", "20170228", fallback = true)
        .get
    }
    var expectedMessage =
      "No exchange rate between currencies \"USD\" and \"XXX\" could " +
        "be found even after fallback on previous dates."
    assert(exception.getMessage === expectedMessage)
    // With another format:
    exception = intercept[CurrencyConverterException] {
      currencyConverter
        .exchangeRate("USD", "XXX", "2017-02-28", "yyyy-MM-dd", fallback = true)
        .get
    }
    expectedMessage =
      "No exchange rate between currencies \"USD\" and \"XXX\" could " +
        "be found even after fallback on previous dates."
    assert(exception.getMessage === expectedMessage)
  }

  test("Get Converted Price with Fallback") {

    val currencyConverter = new CurrencyConverter(
      "src/test/resources/hdfs_rates_fallback",
      sc,
      "20170201",
      "20170228")

    // 1: Let's try even if there's no need to fallback:

    var convertedPrice = currencyConverter.convert(2d, "USD", "SEK", "20170228")
    assert(convertedPrice === Success(16.9712d))

    convertedPrice = currencyConverter
      .convert(2d, "USD", "SEK", "20170228", fallback = true)
    assert(convertedPrice === Success(16.9712d))

    convertedPrice = currencyConverter
      .convert(2d, "USD", "SEK", "170228", "yyMMdd", fallback = true)
    assert(convertedPrice === Success(16.9712d))

    // 2: USD to EUR rate is not available for 20170228, and not available for
    // 20170227 but is available for 20170201:

    assert(currencyConverter.convert(2d, "USD", "GBP", "20170228").isFailure)
    assert(currencyConverter.convert(2d, "USD", "GBP", "20170227").isFailure)

    convertedPrice = currencyConverter.convert(2d, "USD", "GBP", "20170201")
    assert(convertedPrice === Success(1.59838d))

    convertedPrice = currencyConverter
      .convert(2d, "USD", "GBP", "20170228", fallback = true)
    assert(convertedPrice === Success(1.59838d))

    convertedPrice = currencyConverter
      .convert(2d, "USD", "GBP", "2017-02-28", "yyyy-MM-dd", fallback = true)
    assert(convertedPrice === Success(1.59838d))

    // 3: Let's check we do get an exception if there are absolutely no dates
    // with the requested rate:
    var exception = intercept[CurrencyConverterException] {
      currencyConverter
        .convert(2d, "USD", "XXX", "20170228", fallback = true)
        .get
    }
    var expectedMessage =
      "No exchange rate between currencies \"USD\" and \"XXX\" could " +
        "be found even after fallback on previous dates."
    assert(exception.getMessage === expectedMessage)
    // With another format
    exception = intercept[CurrencyConverterException] {
      currencyConverter
        .convert(2d, "USD", "XXX", "170228", "yyMMdd", fallback = true)
        .get
    }
    expectedMessage =
      "No exchange rate between currencies \"USD\" and \"XXX\" could " +
        "be found even after fallback on previous dates."
    assert(exception.getMessage === expectedMessage)
  }
}
