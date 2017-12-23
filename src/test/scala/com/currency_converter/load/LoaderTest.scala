package com.currency_converter.load

import com.currency_converter.error.CurrencyConverterException
import com.currency_converter.model.ExchangeRate

import com.holdenkarau.spark.testing.SharedSparkContext

import org.scalatest.FunSuite
import org.scalatest.PrivateMethodTester
import org.scalatest.PrivateMethodTester.PrivateMethod

/** Testing facility for the part loading exchange rates.
  *
  * @author Xavier Guihot
  * @since 2017-02
  */
class LoaderTest extends FunSuite with SharedSparkContext with PrivateMethodTester {

	test("Parse OneRate Line") {

		// 1:
		var rawRate = "20171224,SEK,USD,8.33034829"
		var expectedRate = Some(ExchangeRate("20171224", "SEK", "USD", 8.33034829d))
		assert(Loader.parseDefaultRateLine(rawRate) === expectedRate)

		// 2:
		rawRate = "20170327,USD,CRC,564.85"
		expectedRate = Some(ExchangeRate("20170327", "USD", "CRC", 564.85d))
		assert(Loader.parseDefaultRateLine(rawRate) === expectedRate)
	}

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
			rawRates, Loader.parseDefaultRateLine, "20171224", "20171225"
		)

		val expectedToUsdRates = Map(
			"20171224" -> Map("SEK" -> 0.12004300002683321d, "EUR" -> 1.0573283427435556d),
			"20171225" -> Map("SEK" -> 0.119829215d)
		)

		assert(computedToUSDrates === expectedToUsdRates)
	}

	test("Test all Required Dates of Rates have been Loaded") {

		val checkDataFullAvailibility = PrivateMethod[Unit]('checkDataFullAvailibility)

		// 1: All right, no exception raised:
		Loader invokePrivate checkDataFullAvailibility(
			Map(
				"20171224" -> Map("SEK" -> 0.120043d, "EUR" -> 1.0573283d),
				"20171225" -> Map("SEK" -> 0.119829215d),
				"20171226" -> Map("EUR" -> 1.25d)
			),
			"20171224", "20171226"
		)

		// 2: Missing date: an exception is raised:
		val exception = intercept[CurrencyConverterException] {
			Loader invokePrivate checkDataFullAvailibility(
				Map(
					"20171224" -> Map("SEK" -> 0.120043d, "EUR" -> 1.0573283d),
					"20171226" -> Map("EUR" -> 1.25d)
				),
				"20171224", "20171226"
			)
		}
		val expectedMessage = (
			"No exchange rate could be loaded for date(s) \"List(20171225)\"."
		)
		assert(exception.getMessage === expectedMessage)
	}

	test("Load Exchange Rates from Hadoop") {

		// 1:
		var computedToUSDrates = Loader.loadExchangeRates(
			Some(sc), "src/test/resources/hdfs_rates",
			Loader.parseDefaultRateLine, "20170227", "20170228", false
		)
		var expectedToUsdRates = Map(
			"20170227" -> Map("SEK" -> 0.12004300002683321d, "EUR" -> 0.8d),
			"20170228" -> Map("SEK" -> 0.11784670500612802d, "EUR" -> 0.8092710086753853d)
		)
		assert(computedToUSDrates === expectedToUsdRates)

		// 2: Missing rate date:
		val exception = intercept[CurrencyConverterException] {
			Loader.loadExchangeRates(
				Some(sc), "src/test/resources/hdfs_rates",
				Loader.parseDefaultRateLine, "20170226", "20170228", false
			)
		}
		val expectedMessage = (
			"No exchange rate could be loaded for date(s) \"List(20170226)\"."
		)

		// 3: Missing rate date, but we specified it wasn't a problem:
		computedToUSDrates = Loader.loadExchangeRates(
			Some(sc), "src/test/resources/hdfs_rates",
			Loader.parseDefaultRateLine, "20170226", "20170228", true
		)
		assert(computedToUSDrates === expectedToUsdRates)
	}

	test("Load Exchange Rates from a Classic File System") {

		// 1:
		var computedToUSDrates = Loader.loadExchangeRates(
			None, "src/test/resources/hdfs_rates",
			Loader.parseDefaultRateLine, "20170227", "20170228", false
		)
		var expectedToUsdRates = Map(
			"20170227" -> Map("SEK" -> 0.12004300002683321d, "EUR" -> 0.8d),
			"20170228" -> Map("SEK" -> 0.11784670500612802d, "EUR" -> 0.8092710086753853d)
		)
		assert(computedToUSDrates === expectedToUsdRates)

		// 2: Missing rate date:
		val exception = intercept[CurrencyConverterException] {
			Loader.loadExchangeRates(
				None, "src/test/resources/hdfs_rates",
				Loader.parseDefaultRateLine, "20170226", "20170228", false
			)
		}
		val expectedMessage = (
			"No exchange rate could be loaded for date(s) \"List(20170226)\"."
		)

		// 3: Missing rate date, but we specified it wasn't a problem:
		computedToUSDrates = Loader.loadExchangeRates(
			None, "src/test/resources/hdfs_rates",
			Loader.parseDefaultRateLine, "20170226", "20170228", true
		)
		assert(computedToUSDrates === expectedToUsdRates)
	}
}
