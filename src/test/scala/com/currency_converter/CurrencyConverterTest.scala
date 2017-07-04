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
		assert(currencyConverter.getExchangeRate("USD", "EUR", "20170201") === 0.9317799f)
		assert(currencyConverter.getExchangeRate("USD", "USD", "20170201") === 1f)
		assert(currencyConverter.getExchangeRate("USD", "SEK", "20170201") === 8.80033f)
		assert(currencyConverter.getExchangeRate("USD", "SEK", "20170201", "yyyyMMdd") === 8.80033f)
		assert(currencyConverter.getExchangeRate("USD", "EUR", "2017-02-01", "yyyy-MM-dd") === 0.9317799f)
		assert(currencyConverter.getExchangeRate("USD", "USD", "2017-02-01", "yyyy-MM-dd") === 1f)
		assert(currencyConverter.getExchangeRate("USD", "SEK", "2017-02-01", "yyyy-MM-dd") === 8.80033f)
		assert(currencyConverter.getExchangeRate("USD", "EUR", "170201", "yyMMdd") === 0.9317799f)

		// Opposite:
		assert(currencyConverter.getExchangeRate("EUR", "USD", "20170201") === 1.0732148f)
		assert(currencyConverter.getExchangeRate("SEK", "USD", "20170201") === 0.1136321f)
		assert(currencyConverter.getExchangeRate("EUR", "USD", "2017-02-01", "yyyy-MM-dd") === 1.0732148f)
		assert(currencyConverter.getExchangeRate("SEK", "USD", "2017-02-01", "yyyy-MM-dd") === 0.1136321f)

		// With something else than USD:
		assert(currencyConverter.getExchangeRate("EUR", "SEK", "20170201") === 9.444644f)
		assert(currencyConverter.getExchangeRate("EUR", "SEK", "2017-02-01", "yyyy-MM-dd") === 9.444644f)

		// Usage of the orElse alias:
		assert(currencyConverter.getExchangeRateOrElse("USD", "EUR", "20170201", 1f) === 0.9317799f)
		assert(currencyConverter.getExchangeRateOrElse("USD", "USD", "20170201", 1f) === 1f)
		assert(currencyConverter.getExchangeRateOrElse("EUR", "SEK", "20170201", 1f) === 9.444644f)
		assert(currencyConverter.getExchangeRateOrElse("USD", "EUR", "2017-02-01", 1f, "yyyy-MM-dd") === 0.9317799f)
		assert(currencyConverter.getExchangeRateOrElse("USD", "USD", "2017-02-01", 1f, "yyyy-MM-dd") === 1f)
		assert(currencyConverter.getExchangeRateOrElse("EUR", "SEK", "2017-02-01", 1f, "yyyy-MM-dd") === 9.444644f)

		// With a non existing date:
		assert(currencyConverter.getExchangeRateOrElse("EUR", "SEK", "20170202", 1f) === 1f)
		assert(currencyConverter.getExchangeRateOrElse("EUR", "SEK", "20170202", 0f) === 0f)
		assert(currencyConverter.getExchangeRateOrElse("EUR", "SEK", "2017-02-02", 1f, "yyyy-MM-dd") === 1f)
		assert(currencyConverter.getExchangeRateOrElse("EUR", "SEK", "2017-02-02", 0f, "yyyy-MM-dd") === 0f)
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
		assert(currencyConverter.getExchangeRateOrElse("...", "SEK", "20170202", 1f) === 1f)
		assert(currencyConverter.getExchangeRateOrElse("...", "SEK", "2017-02-02", 1f, "yyyy-MM-dd") === 1f)
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

		assert(currencyConverter.getExchangeRate("USD", "EUR", "20170201") === 0.9317799f)
		assert(currencyConverter.getExchangeRate("USD", "USD", "20170201") === 1f)
		assert(currencyConverter.getExchangeRate("USD", "SEK", "20170201") === 8.80033f)
		assert(currencyConverter.getExchangeRate("EUR", "USD", "20170201") === 1.0732148f)
		assert(currencyConverter.getExchangeRate("EUR", "SEK", "2017-02-01", "yyyy-MM-dd") === 9.444644f)
		assert(currencyConverter.getExchangeRateOrElse("EUR", "SEK", "2017-02-01", 1f, "yyyy-MM-dd") === 9.444644f)
		assert(currencyConverter.getExchangeRateOrElse("EUR", "SEK", "20170202", 1f) === 1f)
		assert(currencyConverter.getExchangeRateOrElse("EUR", "SEK", "2017-02-02", 1f, "yyyy-MM-dd") === 1f)
	}

	test("Get Exchange Rate after Loading Rates from a Classic File System") {

		val currencyConverter = new CurrencyConverter(
			currencyFolder = "src/test/resources/hdfs_rates",
			firstDateOfRates = "20170201", lastDateOfRates = "20170201"
		)

		assert(currencyConverter.getExchangeRate("USD", "EUR", "20170201") === 0.9317799f)
		assert(currencyConverter.getExchangeRate("USD", "USD", "20170201") === 1f)
		assert(currencyConverter.getExchangeRate("USD", "SEK", "20170201") === 8.80033f)
	}

	test("Get Converted Price") {

		val currencyConverter = new CurrencyConverter(
			currencyFolder = "src/test/resources/hdfs_rates",
			sparkContext = Some(sc),
			firstDateOfRates = "20170201", lastDateOfRates = "20170201"
		)

		assert(currencyConverter.convert(1f, "USD", "USD", "20170201") === 1f)
		assert(currencyConverter.convert(1f, "EUR", "USD", "20170201") === 1.0732148f)
		assert(currencyConverter.convert(1f, "EUR", "SEK", "20170201") === 9.444644f)
		assert(currencyConverter.convert(1f, "EUR", "SEK", "20170201", "yyyyMMdd") === 9.444644f)

		assert(currencyConverter.convert(1f, "USD", "USD", "170201", "yyMMdd") === 1f)
		assert(currencyConverter.convert(1f, "EUR", "USD", "2017-02-01", "yyyy-MM-dd") === 1.0732148f)
		assert(currencyConverter.convert(1f, "EUR", "SEK", "170201", "yyMMdd") === 9.444644f)

		assert(currencyConverter.convert(12.5f, "USD", "USD", "20170201") === 12.5f)
		assert(currencyConverter.convert(12.5f, "EUR", "USD", "20170201") === 13.415185f)
		assert(currencyConverter.convert(12.5f, "EUR", "SEK", "20170201") === 118.05805f)

		assert(currencyConverter.convertOrElse(12.5f, "USD", "USD", "20170201", 12.5f) === 12.5f)
		assert(currencyConverter.convertOrElse(12.5f, "EUR", "USD", "20170201", 12.5f) === 13.415185f)
		assert(currencyConverter.convertOrElse(12.5f, "EUR", "SEK", "20170201", 12.5f) === 118.05805f)

		// With a non existing date (this is all taken care of by getExchangeRate,
		// but let's test it anyway):
		assert(currencyConverter.convertOrElse(12.5f, "EUR", "SEK", "20170202", 12.5f) === 12.5f)
		assert(currencyConverter.convertOrElse(12.5f, "EUR", "SEK", "20170202", 0f) === 0f)
		assert(currencyConverter.convertOrElse(12.5f, "EUR", "SEK", "2017-02-02", 0f, "yyyy-MM-dd") === 0f)
		var exception = intercept[CurrencyConverterException] {
			currencyConverter.convert(12.5f, "EUR", "SEK", "20170202")
		}
		var expectedMessage = (
			"No exchange rate for currency \"EUR\" for date \"20170202\"."
		)
		assert(exception.getMessage === expectedMessage)
		exception = intercept[CurrencyConverterException] {
			currencyConverter.convert(12.5f, "EUR", "SEK", "170202", "yyMMdd")
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
		assert(exchangeRate === 8.4856f)
		exchangeRate = currencyConverter.getExchangeRateAndFallback("USD", "SEK", "20170228")
		assert(exchangeRate === 8.4856f)
		exchangeRate = currencyConverter.getExchangeRateAndFallback("USD", "SEK", "2017-02-28", "yyyy-MM-dd")
		assert(exchangeRate === 8.4856f)
		var exchangeRateTuple = currencyConverter.getExchangeRateAndFallbackWithDetail(
			"USD", "SEK", "20170228"
		)
		assert(exchangeRateTuple === (8.4856f, "20170228"))
		exchangeRateTuple = currencyConverter.getExchangeRateAndFallbackWithDetail(
			"USD", "SEK", "20170228", "yyyyMMdd"
		)
		assert(exchangeRateTuple === (8.4856f, "20170228"))
		exchangeRateTuple = currencyConverter.getExchangeRateAndFallbackWithDetail(
			"USD", "SEK", "2017-02-28", "yyyy-MM-dd"
		)
		assert(exchangeRateTuple === (8.4856f, "20170228"))

		// 2: USD to EUR rate is not available for 20170228, but is available for
		// 20170227:
		intercept[CurrencyConverterException] {
			currencyConverter.getExchangeRate("USD", "EUR", "20170228")
		}
		assert(currencyConverter.getExchangeRate("USD", "EUR", "20170227") === 1.25f)
		exchangeRate = currencyConverter.getExchangeRateAndFallback(
			"USD", "EUR", "20170228"
		)
		assert(exchangeRate === 1.25f)
		exchangeRate = currencyConverter.getExchangeRateAndFallback(
			"USD", "EUR", "170228", "yyMMdd"
		)
		assert(exchangeRate === 1.25f)
		exchangeRateTuple = currencyConverter.getExchangeRateAndFallbackWithDetail(
			"USD", "EUR", "20170228"
		)
		assert(exchangeRateTuple === (1.25f, "20170227"))
		exchangeRateTuple = currencyConverter.getExchangeRateAndFallbackWithDetail(
			"USD", "EUR", "170228", "yyMMdd"
		)
		assert(exchangeRateTuple === (1.25f, "20170227"))

		// 3: USD to EUR rate is not available for 20170228, and not availbale for
		// 20170227 but is available for 20170201:
		intercept[CurrencyConverterException] {
			currencyConverter.getExchangeRate("USD", "GBP", "20170228")
		}
		intercept[CurrencyConverterException] {
			currencyConverter.getExchangeRate("USD", "GBP", "20170227")
		}
		assert(currencyConverter.getExchangeRate("USD", "GBP", "20170201") === 0.79919f)
		exchangeRate = currencyConverter.getExchangeRateAndFallback(
			"USD", "GBP", "20170228"
		)
		assert(exchangeRate === 0.79919f)
		exchangeRate = currencyConverter.getExchangeRateAndFallback(
			"USD", "GBP", "2017-02-28", "yyyy-MM-dd"
		)
		assert(exchangeRate === 0.79919f)
		exchangeRateTuple = currencyConverter.getExchangeRateAndFallbackWithDetail(
			"USD", "GBP", "20170228"
		)
		assert(exchangeRateTuple === (0.79919f, "20170201"))
		exchangeRateTuple = currencyConverter.getExchangeRateAndFallbackWithDetail(
			"USD", "GBP", "170228", "yyMMdd"
		)
		assert(exchangeRateTuple === (0.79919f, "20170201"))

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
			2f, "USD", "SEK", "20170228"
		)
		assert(convertedPrice === 16.9712f)
		convertedPrice = currencyConverter.convertAndFallback(
			2f, "USD", "SEK", "20170228"
		)
		assert(convertedPrice === 16.9712f)
		convertedPrice = currencyConverter.convertAndFallback(
			2f, "USD", "SEK", "170228", "yyMMdd"
		)
		assert(convertedPrice === 16.9712f)
		var convertedPriceTuple = currencyConverter.convertAndFallbackWithDetail(
			2f, "USD", "SEK", "20170228"
		)
		assert(convertedPriceTuple === (16.9712f, "20170228"))
		convertedPriceTuple = currencyConverter.convertAndFallbackWithDetail(
			2f, "USD", "SEK", "2017-02-28", "yyyy-MM-dd"
		)
		assert(convertedPriceTuple === (16.9712f, "20170228"))

		// 2: USD to EUR rate is not available for 20170228, and not availbale for
		// 20170227 but is available for 20170201:
		intercept[CurrencyConverterException] {
			currencyConverter.convert(2f, "USD", "GBP", "20170228")
		}
		intercept[CurrencyConverterException] {
			currencyConverter.convert(2f, "USD", "GBP", "20170227")
		}
		assert(currencyConverter.convert(2f, "USD", "GBP", "20170201") === 1.59838f)
		convertedPrice = currencyConverter.convertAndFallback(
			2f, "USD", "GBP", "20170228"
		)
		assert(convertedPrice === 1.59838f)
		convertedPrice = currencyConverter.convertAndFallback(
			2f, "USD", "GBP", "2017-02-28", "yyyy-MM-dd"
		)
		assert(convertedPrice === 1.59838f)
		convertedPriceTuple = currencyConverter.convertAndFallbackWithDetail(
			2f, "USD", "GBP", "20170228"
		)
		assert(convertedPriceTuple === (1.59838f, "20170201"))
		convertedPriceTuple = currencyConverter.convertAndFallbackWithDetail(
			2f, "USD", "GBP", "170228", "yyMMdd"
		)
		assert(convertedPriceTuple === (1.59838f, "20170201"))

		// 3: Let's check we do get an exception if there are absolutly no dates
		// with the requested rate:
		var exception = intercept[CurrencyConverterException] {
			currencyConverter.convertAndFallbackWithDetail(
				2f, "USD", "XXX", "20170228"
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
				2f, "USD", "XXX", "170228", "yyMMdd"
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
			currencyConverter.convert(1f, "USD", "USD", "170201")
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
			currencyConverter.convert(1f, "USD", "USD", "20170201", "yyyy-MM-dd")
		}
		expectedMessage = (
			"Date \"20170201\" doesn't look like a yyyy-MM-dd date."
		)
		assert(exception.getMessage === expectedMessage)

		// 4: During a conversion with fallback:
		exception = intercept[IllegalArgumentException] {
			currencyConverter.convertAndFallback(
				1f, "USD", "USD", "20170201", "yyyy-MM-dd"
			)
		}
		expectedMessage = (
			"Date \"20170201\" doesn't look like a yyyy-MM-dd date."
		)
		assert(exception.getMessage === expectedMessage)
	}
}
