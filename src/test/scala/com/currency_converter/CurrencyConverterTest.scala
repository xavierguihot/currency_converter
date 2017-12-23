package com.currency_converter

import com.currency_converter.error.CurrencyConverterException
import com.currency_converter.model.ExchangeRate

import com.holdenkarau.spark.testing.SharedSparkContext

import org.scalatest.FunSuite

/** Testing facility for Currency Convertion base methods.
  *
  * @author Xavier Guihot
  * @since 2017-02
  */
class CurrencyConverterTest extends FunSuite with SharedSparkContext {

	test("Get Exchange Rate") {

		val currencyConverter = new CurrencyConverter(
			currencyFolder = "src/test/resources/hdfs_rates",
			sparkContext = Some(sc),
			firstDateOfRates = "20170201", lastDateOfRates = "20170201"
		)

		// Direct application of what's in the rate file:
		assert(currencyConverter.getExchangeRate("USD", "EUR", "20170201") === 0.93178d)
		assert(currencyConverter.getExchangeRate("USD", "USD", "20170201") === 1d)
		assert(currencyConverter.getExchangeRate("USD", "SEK", "20170201") === 8.80033d)
		assert(currencyConverter.getExchangeRate("USD", "SEK", "20170201", "yyyyMMdd") === 8.80033d)
		assert(currencyConverter.getExchangeRate("USD", "EUR", "2017-02-01", "yyyy-MM-dd") === 0.93178d)
		assert(currencyConverter.getExchangeRate("USD", "USD", "2017-02-01", "yyyy-MM-dd") === 1d)
		assert(currencyConverter.getExchangeRate("USD", "SEK", "2017-02-01", "yyyy-MM-dd") === 8.80033d)
		assert(currencyConverter.getExchangeRate("USD", "EUR", "170201", "yyMMdd") === 0.93178d)

		// Opposite:
		assert(currencyConverter.getExchangeRate("EUR", "USD", "20170201") === 1.0732147073343492d)
		assert(currencyConverter.getExchangeRate("SEK", "USD", "20170201") === 0.1136321024325224d)
		assert(currencyConverter.getExchangeRate("EUR", "USD", "2017-02-01", "yyyy-MM-dd") === 1.0732147073343492d)
		assert(currencyConverter.getExchangeRate("SEK", "USD", "2017-02-01", "yyyy-MM-dd") === 0.1136321024325224d)

		// With something else than USD:
		assert(currencyConverter.getExchangeRate("EUR", "SEK", "20170201") === 9.444643585395694d)
		assert(currencyConverter.getExchangeRate("EUR", "SEK", "2017-02-01", "yyyy-MM-dd") === 9.444643585395694d)

		// Usage of the orElse alias:
		assert(currencyConverter.getExchangeRateOrElse("USD", "EUR", "20170201", 1d) === 0.93178d)
		assert(currencyConverter.getExchangeRateOrElse("USD", "USD", "20170201", 1d) === 1d)
		assert(currencyConverter.getExchangeRateOrElse("EUR", "SEK", "20170201", 1d) === 9.444643585395694d)
		assert(currencyConverter.getExchangeRateOrElse("USD", "EUR", "2017-02-01", 1d, "yyyy-MM-dd") === 0.93178d)
		assert(currencyConverter.getExchangeRateOrElse("USD", "USD", "2017-02-01", 1d, "yyyy-MM-dd") === 1d)
		assert(currencyConverter.getExchangeRateOrElse("EUR", "SEK", "2017-02-01", 1d, "yyyy-MM-dd") === 9.444643585395694d)

		// With a non existing date:
		assert(currencyConverter.getExchangeRateOrElse("EUR", "SEK", "20170202", 1d) === 1d)
		assert(currencyConverter.getExchangeRateOrElse("EUR", "SEK", "20170202", 0d) === 0d)
		assert(currencyConverter.getExchangeRateOrElse("EUR", "SEK", "2017-02-02", 1d, "yyyy-MM-dd") === 1d)
		assert(currencyConverter.getExchangeRateOrElse("EUR", "SEK", "2017-02-02", 0d, "yyyy-MM-dd") === 0d)
		var exception = intercept[CurrencyConverterException] {
			currencyConverter.getExchangeRate("EUR", "SEK", "20170202")
		}
		var expectedMessage = (
			"No exchange rate for currency \"EUR\" for date \"20170202\"."
		)
		assert(exception.getMessage === expectedMessage)
		exception = intercept[CurrencyConverterException] {
			currencyConverter.getExchangeRate("EUR", "SEK", "2017-02-02", "yyyy-MM-dd")
		}
		expectedMessage = (
			"No exchange rate for currency \"EUR\" for date \"20170202\"."
		)
		assert(exception.getMessage === expectedMessage)

		// With a missing currency:
		assert(currencyConverter.getExchangeRateOrElse("...", "SEK", "20170202", 1d) === 1d)
		assert(currencyConverter.getExchangeRateOrElse("...", "SEK", "2017-02-02", 1d, "yyyy-MM-dd") === 1d)
		exception = intercept[CurrencyConverterException] {
			currencyConverter.getExchangeRate("...", "SEK", "20170201")
		}
		expectedMessage = (
			"No exchange rate for currency \"...\" for date \"20170201\"."
		)
		assert(exception.getMessage === expectedMessage)
		exception = intercept[CurrencyConverterException] {
			currencyConverter.getExchangeRate("...", "SEK", "2017-02-01", "yyyy-MM-dd")
		}
		expectedMessage = (
			"No exchange rate for currency \"...\" for date \"20170201\"."
		)
		assert(exception.getMessage === expectedMessage)

		// With a non existing currency, but from this currency to the same
		// currency:
		assert(currencyConverter.getExchangeRate("XXX", "XXX", "20110719") === 1d)
	}

	test("Get Exchange Rate after Loading with a Custom Rate Format") {

		// The custom format is:
		// 		2017-02-01,USD,,EUR,,,0.93178
		// instead of the default one which would be:
		// 		20170201,USD,EUR,0.93178
		// Notice how we use a function here and not a method (val instead of
		// def), because otherwise, the method is not Serializable and this will
		// throw a not Serializable exception at run time:
		val customParseRateLine = (rawRateLine: String) => {

			val splittedRateLine = rawRateLine.split("\\,", -1)

			val date = splittedRateLine(0).replace("-", "")
			val fromCurrency = splittedRateLine(1)
			val toCurrency = splittedRateLine(3)
			val exchangeRate = splittedRateLine(6).toFloat

			Some(ExchangeRate(date, fromCurrency, toCurrency, exchangeRate))
		}

		val currencyConverter = new CurrencyConverter(
			currencyFolder = "src/test/resources/hdfs_rates_custom_format",
			sparkContext = Some(sc), parseRateLine = customParseRateLine,
			firstDateOfRates = "20170201", lastDateOfRates = "20170201"
		)

		assert(currencyConverter.getExchangeRate("USD", "EUR", "20170201") === 0.9317799806594848d)
		assert(currencyConverter.getExchangeRate("USD", "USD", "20170201") === 1d)
		assert(currencyConverter.getExchangeRate("USD", "SEK", "20170201") === 8.80033016204834d)
		assert(currencyConverter.getExchangeRate("EUR", "USD", "20170201") === 1.073214729610558d)
		assert(currencyConverter.getExchangeRate("EUR", "SEK", "2017-02-01", "yyyy-MM-dd") === 9.444643955346347d)
		assert(currencyConverter.getExchangeRateOrElse("EUR", "SEK", "2017-02-01", 1d, "yyyy-MM-dd") === 9.444643955346347d)
		assert(currencyConverter.getExchangeRateOrElse("EUR", "SEK", "20170202", 1d) === 1d)
		assert(currencyConverter.getExchangeRateOrElse("EUR", "SEK", "2017-02-02", 1d, "yyyy-MM-dd") === 1d)
	}

	test("Get Exchange Rate after Loading Rates from a Classic File System") {

		val currencyConverter = new CurrencyConverter(
			currencyFolder = "src/test/resources/hdfs_rates",
			firstDateOfRates = "20170201", lastDateOfRates = "20170201"
		)

		assert(currencyConverter.getExchangeRate("USD", "EUR", "20170201") === 0.93178d)
		assert(currencyConverter.getExchangeRate("USD", "USD", "20170201") === 1d)
		assert(currencyConverter.getExchangeRate("USD", "SEK", "20170201") === 8.80033d)
	}

	test("Get Converted Price") {

		val currencyConverter = new CurrencyConverter(
			currencyFolder = "src/test/resources/hdfs_rates",
			sparkContext = Some(sc),
			firstDateOfRates = "20170201", lastDateOfRates = "20170201"
		)

		assert(currencyConverter.convert(1d, "USD", "USD", "20170201") === 1d)
		assert(currencyConverter.convert(1d, "EUR", "USD", "20170201") === 1.0732147073343492d)
		assert(currencyConverter.convert(1d, "EUR", "SEK", "20170201") === 9.444643585395694d)
		assert(currencyConverter.convert(1d, "EUR", "SEK", "20170201", "yyyyMMdd") === 9.444643585395694d)

		assert(currencyConverter.convert(1d, "USD", "USD", "170201", "yyMMdd") === 1d)
		assert(currencyConverter.convert(1d, "EUR", "USD", "2017-02-01", "yyyy-MM-dd") === 1.0732147073343492d)
		assert(currencyConverter.convert(1d, "EUR", "SEK", "170201", "yyMMdd") === 9.444643585395694d)

		assert(currencyConverter.convert(12.5d, "USD", "USD", "20170201") === 12.5d)
		assert(currencyConverter.convert(12.5d, "EUR", "USD", "20170201") === 13.415183841679365d)
		assert(currencyConverter.convert(12.5d, "EUR", "SEK", "20170201") === 118.05804481744617d)

		assert(currencyConverter.convertOrElse(12.5d, "USD", "USD", "20170201", 12.5d) === 12.5d)
		assert(currencyConverter.convertOrElse(12.5d, "EUR", "USD", "20170201", 12.5d) === 13.415183841679365d)
		assert(currencyConverter.convertOrElse(12.5d, "EUR", "SEK", "20170201", 12.5d) === 118.05804481744617d)

		// With a non existing date (this is all taken care of by getExchangeRate,
		// but let's test it anyway):
		assert(currencyConverter.convertOrElse(12.5d, "EUR", "SEK", "20170202", 12.5d) === 12.5d)
		assert(currencyConverter.convertOrElse(12.5d, "EUR", "SEK", "20170202", 0d) === 0d)
		assert(currencyConverter.convertOrElse(12.5d, "EUR", "SEK", "2017-02-02", 0d, "yyyy-MM-dd") === 0d)
		var exception = intercept[CurrencyConverterException] {
			currencyConverter.convert(12.5d, "EUR", "SEK", "20170202")
		}
		var expectedMessage = (
			"No exchange rate for currency \"EUR\" for date \"20170202\"."
		)
		assert(exception.getMessage === expectedMessage)
		exception = intercept[CurrencyConverterException] {
			currencyConverter.convert(12.5d, "EUR", "SEK", "170202", "yyMMdd")
		}
		expectedMessage = (
			"No exchange rate for currency \"EUR\" for date \"20170202\"."
		)
		assert(exception.getMessage === expectedMessage)
	}

	test("Get Exchange Rate with Fallback") {

		val currencyConverter = new CurrencyConverter(
			currencyFolder = "src/test/resources/hdfs_rates_fallback",
			sparkContext = Some(sc), tolerateUnexpectedMissingRateFiles = true,
			firstDateOfRates = "20170201", lastDateOfRates = "20170228"
		)

		// 1: Let's try even if there's no need to fallback:
		var exchangeRate = currencyConverter.getExchangeRate("USD", "SEK", "20170228")
		assert(exchangeRate === 8.4856d)
		exchangeRate = currencyConverter.getExchangeRateAndFallback("USD", "SEK", "20170228")
		assert(exchangeRate === 8.4856d)
		exchangeRate = currencyConverter.getExchangeRateAndFallback("USD", "SEK", "2017-02-28", "yyyy-MM-dd")
		assert(exchangeRate === 8.4856d)
		var exchangeRateTuple = currencyConverter.getExchangeRateAndFallbackWithDetail(
			"USD", "SEK", "20170228"
		)
		assert(exchangeRateTuple === (8.4856d, "20170228"))
		exchangeRateTuple = currencyConverter.getExchangeRateAndFallbackWithDetail(
			"USD", "SEK", "20170228", "yyyyMMdd"
		)
		assert(exchangeRateTuple === (8.4856d, "20170228"))
		exchangeRateTuple = currencyConverter.getExchangeRateAndFallbackWithDetail(
			"USD", "SEK", "2017-02-28", "yyyy-MM-dd"
		)
		assert(exchangeRateTuple === (8.4856d, "20170228"))

		// 2: USD to EUR rate is not available for 20170228, but is available for
		// 20170227:
		intercept[CurrencyConverterException] {
			currencyConverter.getExchangeRate("USD", "EUR", "20170228")
		}
		assert(currencyConverter.getExchangeRate("USD", "EUR", "20170227") === 1.25d)
		exchangeRate = currencyConverter.getExchangeRateAndFallback(
			"USD", "EUR", "20170228"
		)
		assert(exchangeRate === 1.25d)
		exchangeRate = currencyConverter.getExchangeRateAndFallback(
			"USD", "EUR", "170228", "yyMMdd"
		)
		assert(exchangeRate === 1.25d)
		exchangeRateTuple = currencyConverter.getExchangeRateAndFallbackWithDetail(
			"USD", "EUR", "20170228"
		)
		assert(exchangeRateTuple === (1.25d, "20170227"))
		exchangeRateTuple = currencyConverter.getExchangeRateAndFallbackWithDetail(
			"USD", "EUR", "170228", "yyMMdd"
		)
		assert(exchangeRateTuple === (1.25d, "20170227"))

		// 3: USD to EUR rate is not available for 20170228, and not availbale for
		// 20170227 but is available for 20170201:
		intercept[CurrencyConverterException] {
			currencyConverter.getExchangeRate("USD", "GBP", "20170228")
		}
		intercept[CurrencyConverterException] {
			currencyConverter.getExchangeRate("USD", "GBP", "20170227")
		}
		assert(currencyConverter.getExchangeRate("USD", "GBP", "20170201") === 0.79919d)
		exchangeRate = currencyConverter.getExchangeRateAndFallback(
			"USD", "GBP", "20170228"
		)
		assert(exchangeRate === 0.79919d)
		exchangeRate = currencyConverter.getExchangeRateAndFallback(
			"USD", "GBP", "2017-02-28", "yyyy-MM-dd"
		)
		assert(exchangeRate === 0.79919d)
		exchangeRateTuple = currencyConverter.getExchangeRateAndFallbackWithDetail(
			"USD", "GBP", "20170228"
		)
		assert(exchangeRateTuple === (0.79919d, "20170201"))
		exchangeRateTuple = currencyConverter.getExchangeRateAndFallbackWithDetail(
			"USD", "GBP", "170228", "yyMMdd"
		)
		assert(exchangeRateTuple === (0.79919d, "20170201"))

		// 4: Let's check we do get an exception if there are absolutly no dates
		// with the requested rate:
		var exception = intercept[CurrencyConverterException] {
			currencyConverter.getExchangeRateAndFallbackWithDetail(
				"USD", "XXX", "20170228"
			)
		}
		var expectedMessage = (
			"No exchange rate for one of the currencies \"USD\" or \"XXX\" " +
			"and no fallback could be found on previous dates"
		)
		assert(exception.getMessage === expectedMessage)
		// With another format:
		exception = intercept[CurrencyConverterException] {
			currencyConverter.getExchangeRateAndFallbackWithDetail(
				"USD", "XXX", "2017-02-28", "yyyy-MM-dd"
			)
		}
		expectedMessage = (
			"No exchange rate for one of the currencies \"USD\" or \"XXX\" " +
			"and no fallback could be found on previous dates"
		)
		assert(exception.getMessage === expectedMessage)
	}

	test("Get Converted Price with Fallback") {

		val currencyConverter = new CurrencyConverter(
			currencyFolder = "src/test/resources/hdfs_rates_fallback",
			sparkContext = Some(sc), tolerateUnexpectedMissingRateFiles = true,
			firstDateOfRates = "20170201", lastDateOfRates = "20170228"
		)

		// 1: Let's try even if there's no need to fallback:
		var convertedPrice = currencyConverter.convert(
			2d, "USD", "SEK", "20170228"
		)
		assert(convertedPrice === 16.9712d)
		convertedPrice = currencyConverter.convertAndFallback(
			2d, "USD", "SEK", "20170228"
		)
		assert(convertedPrice === 16.9712d)
		convertedPrice = currencyConverter.convertAndFallback(
			2d, "USD", "SEK", "170228", "yyMMdd"
		)
		assert(convertedPrice === 16.9712d)
		var convertedPriceTuple = currencyConverter.convertAndFallbackWithDetail(
			2d, "USD", "SEK", "20170228"
		)
		assert(convertedPriceTuple === (16.9712d, "20170228"))
		convertedPriceTuple = currencyConverter.convertAndFallbackWithDetail(
			2d, "USD", "SEK", "2017-02-28", "yyyy-MM-dd"
		)
		assert(convertedPriceTuple === (16.9712d, "20170228"))

		// 2: USD to EUR rate is not available for 20170228, and not availbale for
		// 20170227 but is available for 20170201:
		intercept[CurrencyConverterException] {
			currencyConverter.convert(2d, "USD", "GBP", "20170228")
		}
		intercept[CurrencyConverterException] {
			currencyConverter.convert(2d, "USD", "GBP", "20170227")
		}
		assert(currencyConverter.convert(2d, "USD", "GBP", "20170201") === 1.59838d)
		convertedPrice = currencyConverter.convertAndFallback(
			2d, "USD", "GBP", "20170228"
		)
		assert(convertedPrice === 1.59838d)
		convertedPrice = currencyConverter.convertAndFallback(
			2d, "USD", "GBP", "2017-02-28", "yyyy-MM-dd"
		)
		assert(convertedPrice === 1.59838d)
		convertedPriceTuple = currencyConverter.convertAndFallbackWithDetail(
			2d, "USD", "GBP", "20170228"
		)
		assert(convertedPriceTuple === (1.59838d, "20170201"))
		convertedPriceTuple = currencyConverter.convertAndFallbackWithDetail(
			2d, "USD", "GBP", "170228", "yyMMdd"
		)
		assert(convertedPriceTuple === (1.59838d, "20170201"))

		// 3: Let's check we do get an exception if there are absolutly no dates
		// with the requested rate:
		var exception = intercept[CurrencyConverterException] {
			currencyConverter.convertAndFallbackWithDetail(
				2d, "USD", "XXX", "20170228"
			)
		}
		var expectedMessage = (
			"No exchange rate for one of the currencies \"USD\" or \"XXX\" " +
			"and no fallback could be found on previous dates"
		)
		assert(exception.getMessage === expectedMessage)
		// With another format
		exception = intercept[CurrencyConverterException] {
			currencyConverter.convertAndFallbackWithDetail(
				2d, "USD", "XXX", "170228", "yyMMdd"
			)
		}
		expectedMessage = (
			"No exchange rate for one of the currencies \"USD\" or \"XXX\" " +
			"and no fallback could be found on previous dates"
		)
		assert(exception.getMessage === expectedMessage)
	}

	test("Validate User Requested Date Format") {

		val currencyConverter = new CurrencyConverter(
			currencyFolder = "src/test/resources/hdfs_rates",
			sparkContext = Some(sc),
			firstDateOfRates = "20170201", lastDateOfRates = "20170201"
		)

		// 1: During a conversion:
		var exception = intercept[IllegalArgumentException] {
			currencyConverter.convert(1d, "USD", "EUR", "170201")
		}
		var expectedMessage = (
			"Date \"170201\" doesn't look like a yyyyMMdd date."
		)
		assert(exception.getMessage === expectedMessage)

		// 2: During a rate retrieval:
		exception = intercept[IllegalArgumentException] {
			currencyConverter.getExchangeRate("EUR", "USD", "20170229")
		}
		expectedMessage = (
			"Date \"20170229\" doesn't look like a yyyyMMdd date."
		)
		assert(exception.getMessage === expectedMessage)

		// 3: With another date format than the default one:
		exception = intercept[IllegalArgumentException] {
			currencyConverter.convert(1d, "USD", "EUR", "20170201", "yyyy-MM-dd")
		}
		expectedMessage = (
			"Date \"20170201\" doesn't look like a yyyy-MM-dd date."
		)
		assert(exception.getMessage === expectedMessage)

		// 4: During a conversion with fallback:
		exception = intercept[IllegalArgumentException] {
			currencyConverter.convertAndFallback(
				1d, "USD", "EUR", "20170201", "yyyy-MM-dd"
			)
		}
		expectedMessage = (
			"Date \"20170201\" doesn't look like a yyyy-MM-dd date."
		)
		assert(exception.getMessage === expectedMessage)
	}
}
