package com.currency_converter.load

import com.currency_converter.error.CurrencyConverterException
import com.currency_converter.model.ExchangeRate

import com.holdenkarau.spark.testing.SharedSparkContext

import org.scalatest.FunSuite

/** Testing facility for the part loading exchange rates.
  *
  * @author Xavier Guihot
  * @since 2017-02
  */
class LoaderTest extends FunSuite with SharedSparkContext {

	test("Load Rates from RDD of Raw Rates") {

		val rawRates = sc.parallelize(Array(
			// USD to SEK rate:
			"20171224,USD,SEK,8.33034829",
			// USD to SEK rate (will be overriden by the next rate (duplicate with next line)):
			"20171225,USD,SEK,8.34521",
			// SEL to USD rate for the same date (20171225), but from currency to USD:
			"20171225,SEK,USD,0.119829215",
			// USD to EUR rate:
			"20171224,USD,EUR,0.94578",
			// Outside of requested dates:
			"20171226,USD,SEK,8.45789",
			// Outside of requested dates:
			"20171223,USD,SEK,8.65747",
			// Not a "to USD" or "from USD" exchange rate:
			"20171224,EUR,SEK,9.4"
		))

		val computedToUSDrates = Loader.loadExchangeRatesFromHdfs(
			rawRates, "20171224", "20171225", ExchangeRate.defaultRateLineParser
		)

		val expectedToUsdRates = Map(
			"20171224" -> Map("SEK" -> 0.12004300002683321d, "EUR" -> 1.0573283427435556d),
			"20171225" -> Map("SEK" -> 0.119829215d)
		)

		assert(computedToUSDrates === expectedToUsdRates)
	}

	test("Load Exchange Rates from Hadoop") {

		// 1:
		var computedToUSDrates = Loader.loadExchangeRates(
			"src/test/resources/hdfs_rates", Some(sc),
			"20170227", "20170228", ExchangeRate.defaultRateLineParser
		)
		var expectedToUsdRates = Map(
			"20170227" -> Map("SEK" -> 0.12004300002683321d, "EUR" -> 0.8d),
			"20170228" -> Map("SEK" -> 0.11784670500612802d, "EUR" -> 0.8092710086753853d)
		)
		assert(computedToUSDrates === expectedToUsdRates)

		// 2: Missing rates for 20170226:
		computedToUSDrates = Loader.loadExchangeRates(
			"src/test/resources/hdfs_rates", Some(sc),
			"20170226", "20170228", ExchangeRate.defaultRateLineParser
		)
		assert(computedToUSDrates === expectedToUsdRates)
	}

	test("Load Exchange Rates from a Classic File System") {

		// 1:
		var computedToUSDrates = Loader.loadExchangeRates(
			"src/test/resources/hdfs_rates", None,
			"20170227", "20170228", ExchangeRate.defaultRateLineParser
		)
		var expectedToUsdRates = Map(
			"20170227" -> Map("SEK" -> 0.12004300002683321d, "EUR" -> 0.8d),
			"20170228" -> Map("SEK" -> 0.11784670500612802d, "EUR" -> 0.8092710086753853d)
		)
		assert(computedToUSDrates === expectedToUsdRates)

		// 2: Missing rates for 20170226:
		computedToUSDrates = Loader.loadExchangeRates(
			"src/test/resources/hdfs_rates", None,
			"20170226", "20170228", ExchangeRate.defaultRateLineParser
		)
		assert(computedToUSDrates === expectedToUsdRates)
	}
}
