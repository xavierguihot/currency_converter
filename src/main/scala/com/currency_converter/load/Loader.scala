package com.currency_converter.load

import com.currency_converter.error.CurrencyConverterException
import com.currency_converter.model.ExchangeRate

import org.joda.time.Days
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.io.Source

import java.io.File

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkException

/** Functions called when initializating CurrencyConverter in order to load data.
  *
  * @author Xavier Guihot
  * @since 2017-02
  */
private[currency_converter] object Loader extends Serializable {

	/** Loads data (currency exchange rates).
	  *
	  * Here is a sample of what data looks like once rates are loaded:
	  *
	  * Map(
	  * 	"20170118": Map(
	  * 		"SEK" -> 0.120043f,
	  * 		"CZK" -> 0.041248f,
	  * 		"EUR" -> 1.1145f
	  * 	),
	  * 	"20170119": Map(
	  * 		"SEK" -> 0.12112f,
	  * 		"CZK" -> 0.04175,
	  * 		"EUR" -> 1.10945f
	  * 	)
	  * )
	  *
	  * We only load rates from a currency to USD in order to avoid loading too
	  * much rates into memory (when considering loading a year of rates, if the
	  * cartesian product of all currencies is loaded for each day, this might
	  * start using too much memory).
	  *
	  * @param sparkContext if None, then files are loaded from a classical file
	  * system; if Some(SparkContext), then files are loaded with Spark from
	  * HDFS.
	  * @param currencyFolder the path to the folder which contains currencies
	  * @param parseRateLine the functions which parses a raw exchange rate
	  * @param firstDateOfRates the first date of exchange rates to use
	  * @param lastDateOfRates the last date of exchange rates to use. If it is
	  * "" then the last date is considered to be yesterday.
	  * @param tolerateUnexpectedMissingRateFiles enables not to crash when
	  * dates of exchanges rates are missing in input.
	  * @return the exchange rate map
	  */
	def loadExchangeRates(
		sparkContext: Option[SparkContext], currencyFolder: String,
		parseRateLine: String => Option[ExchangeRate],
		firstDateOfRates: String, lastDateOfRates: String,
		tolerateUnexpectedMissingRateFiles: Boolean
	): Map[String, Map[String, Float]] = {

		// In case the last date of rates to use is "", then it's that the last
		// day to use is yesterday:
		val lastDate = lastDateOfRates match {
			case "" => getYesterday()
			case _  => lastDateOfRates
		}

		// We load data:
		val toUSDrates =
			if (sparkContext.isEmpty)
				loadExchangeRatesFromFs(
					currencyFolder, parseRateLine, firstDateOfRates, lastDate
				)
			else
				loadExchangeRatesFromHdfs(
					sparkContext.get.textFile(currencyFolder), parseRateLine,
					firstDateOfRates, lastDate
				)

		// We check data is entirely here:
		if (!tolerateUnexpectedMissingRateFiles)
			checkDataFullAvailibility(toUSDrates, firstDateOfRates, lastDate)

		// But we also check we have at least one date of rates!:
		if (toUSDrates.isEmpty)
			throw CurrencyConverterException(
				"No exchange rates could be found and thus loaded for all " +
				"requested dates."
			)

		toUSDrates
	}

	/** Loads the rate map from an RDD of rates.
	  *
	  * @param rawRates the RDD of raw rates (such as "2017-03-27,USD,,SEK,,,8.811")
	  * @param parseRateLine the functions which parses a raw exchange rate
	  * @param firstDateOfRates the first date of exchange rates to use
	  * @param lastDateOfRates the last date of exchange rates to use
	  * @return the exchange rate map
	  */
	def loadExchangeRatesFromHdfs(
		rawRates: RDD[String], parseRateLine: String => Option[ExchangeRate],
		firstDateOfRates: String, lastDateOfRates: String
	): Map[String, Map[String, Float]] = {

		rawRates.flatMap(
			rawRate => parseRateLine(rawRate)
		).filter(
			rate => rate.date >= firstDateOfRates && rate.date <= lastDateOfRates
		).filter(
			rate => rate.fromCurrency == "USD" || rate.toCurrency == "USD"
		).groupBy(
			rate => rate.date
		).map {
			case (date, usdRates) => {

				// Rates are transformed to (currency, currencyToUsdRate) tuples:
				val toUsdRates = usdRates.map(
					rate =>
						if (rate.toCurrency == "USD") (rate.fromCurrency, rate.rate)
						else (rate.toCurrency, 1f / rate.rate)
				).toMap

				(date, toUsdRates)
			}
		}.collect().toMap
	}

	/** Loads the rate map from a folder of exchange rate files on a classic file system.
	  *
	  * @param currencyFolder the path to the folder which contains currencies
	  * @param parseRateLine the functions which parses a raw exchange rate
	  * @param firstDateOfRates the first date of exchange rates to use
	  * @param lastDateOfRates the last date of exchange rates to use
	  * @return the exchange rate map
	  */
	def loadExchangeRatesFromFs(
		currencyFolder: String, parseRateLine: String => Option[ExchangeRate],
		firstDateOfRates: String, lastDateOfRates: String
	): Map[String, Map[String, Float]] = {

		val folder = new File(currencyFolder)
		val currencyFiles =
			if (folder.exists && folder.isDirectory)
				folder.listFiles.filter(_.isFile).toList
			else
				List()

		currencyFiles.flatMap(
			currencyFile =>

				Source.fromFile(
					currencyFile, "UTF-8"
				).getLines.flatMap(
					rawRate => parseRateLine(rawRate)
				).filter(
					rate => rate.date >= firstDateOfRates && rate.date <= lastDateOfRates
				).filter(
					rate => rate.fromCurrency == "USD" || rate.toCurrency == "USD"
				)
		).groupBy(
			// Notice how we don't trust the user with the naming of exchange
			// rates files per date, and how we prefer using the field which
			// represents the date within each row:
			rate => rate.date
		).map {
			case (date, usdRates) => {

				// Rates are transformed to (currency, currencyToUsdRate) tuples:
				val toUsdRates = usdRates.map(
					rate =>
						if (rate.toCurrency == "USD") (rate.fromCurrency, rate.rate)
						else (rate.toCurrency, 1f / rate.rate)
				).toMap

				(date, toUsdRates)
			}
		}.toMap
	}

	/** Default parsing of one line of rate.
	  *
	  * If one doesn't modify the method which parses raw exchange rates, then
	  * this is is the applied parser.
	  *
	  * The default format is:
	  * 	yyyyMMddDateOfApplicability,fromCurrency,toCurrency,rate
	  * 	20170327,USD,EUR,0.89
	  *
	  * This returns an Option since users willing to have their xustom RateLine
	  * adapter (to their own rate line format) might want to discard invalid
	  * rate lines, which would thus be filtered by the flatMap calling this
	  * function.
	  *
	  * @param rawRateLine the raw rate to parse, such as:
	  * 	"20170327,USD,EUR,0.89"
	  * @return the parsed rate as an ExchangeRate, such as:
	  * 	Some(ExchangeRate("20170327", "USD", "EUR", 0.89f))
	  */
	def parseDefaultRateLine(rawRateLine: String): Option[ExchangeRate] = {

		val splittedRateLine = rawRateLine.split("\\,", -1)

		val date = splittedRateLine(0)
		val fromCurrency = splittedRateLine(1)
		val toCurrency = splittedRateLine(2)
		val exchangeRate = splittedRateLine(3).toFloat

		Some(ExchangeRate(date, fromCurrency, toCurrency, exchangeRate))
	}

	/** Checks all required dates are available, once data has been loaded.
	  *
	  * @param toUSDrates the map of loaded rates
	  * @param firstDateOfRates the first date of exchange rates to use
	  * @param lastDateOfRates the last date of exchange rates to use
	  */
	def checkDataFullAvailibility(
		toUSDrates: Map[String, Map[String, Float]],
		firstDateOfRates: String, lastDateOfRates: String
	) = {

		val dateFormatter = DateTimeFormat.forPattern("yyyyMMdd")

		val startDate = dateFormatter.parseDateTime(firstDateOfRates)
		val lastDate = dateFormatter.parseDateTime(lastDateOfRates)

		val missingDates = (
			0 to Days.daysBetween(startDate, lastDate).getDays()
		).toList.flatMap(
			dayNbr => {

				val iterationDate = startDate.plusDays(dayNbr)
				val stringDate = dateFormatter.print(iterationDate)

				if (!toUSDrates.contains(stringDate))
					List(stringDate)
				else
					List()
			}
		)

		if (!missingDates.isEmpty)
			throw CurrencyConverterException(
				"No exchange rate could be loaded for date(s) \"" +
				missingDates.toString + "\"."
			)
	}

	/** Retrieve yesterday's date */
	def getYesterday(): String = {
		DateTimeFormat.forPattern("yyyyMMdd").print(new DateTime().minusDays(1))
	}
}
