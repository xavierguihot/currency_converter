package com.currency_converter

import com.currency_converter.error.CurrencyConverterException
import com.currency_converter.load.Loader
import com.currency_converter.model.ExchangeRate

import org.apache.spark.SparkContext

import org.joda.time.format.DateTimeFormat

/** A '''Currency Converter'''.
  *
  * A Scala Wrapper around your exchange rate data for currency conversion.
  *
  * Based on '''your exchange rate files''' stored either on a classic '''file
  * system''' or on '''HDFS''' (Hadoop), this CurrencyConverter object provides
  * for both classic and Spark jobs methods to '''convert prices''' and
  * '''retrieve exchange rates'''.
  *
  * Here are some typical ways of using the currency converter:
  *
  * * You want to use the currency converter within a Spark job. And, you want
  * to have access to the whole history of rates you have (let's say 20140101 to
  * yesterday); and you want an exception to be thrown if for at least one date
  * the CurrencyConverter can't find a single rate, then:
  * {{{
  * val currencyConverter = new CurrencyConverter(
  * 	currencyFolder = "/hdfs/path/to/folder/of/rate/files", sparkContext = Some(sparkContext),
  * 	firstDateOfRates = "20140101", lastDateOfRates = "", tolerateUnexpectedMissingRateFiles = false
  * )
  * // Which is equivalent to:
  * val currencyConverter = new CurrencyConverter(
  * 	"/hdfs/path/to/folder/of/rate/files", Some(sparkContext), "20140101"
  * )
  * // And then:
  * currencyConverter.convert(12.5f, "EUR", "USD", "20170201")
  * currencyConverter.getExchangeRate("EUR", "SEK", "20170201")
  * }}}
  *
  * * You want to use the currency converter without Spark (with rates on a
  * classic file system). And only need rates between 20170228 and 20170327; and
  * you don't want an exception to be thrown if some dates are missing in your
  * source if rates, then:
  * {{{
  * val currencyConverter = new CurrencyConverter(
  * 	currencyFolder = "path/to/folder/of/rate/files", sparkContext = None, firstDateOfRates = "20170228", lastDateOfRates = "20170327",
  * 	tolerateUnexpectedMissingRateFiles = true
  * )
  * // Which is equivalent to:
  * val currencyConverter = new CurrencyConverter(
  * 	"path/to/folder/of/rate/files", firstDateOfRates = "20170228", lastDateOfRates = "20170327",
  * 	tolerateUnexpectedMissingRateFiles = true
  * )
  * // And then:
  * currencyConverter.convert(12.5f, "EUR", "USD", "20170312")
  * currencyConverter.getExchangeRate("EUR", "SEK", "20170301")
  * }}}
  *
  * * You want to use the currency converter within a Spark job. And, you only
  * want to have access to the most up-to-date rates (ideally today's rates) and
  * you can accept to fallback on the previous dates to find a rate when it's
  * not available for the date you've requested, but only if the fallback can be
  * found in the previous week, then:
  * {{{
  * val today = "20170327"
  * val aWeekAgo = "20170320"
  * val currencyConverter = new CurrencyConverter(
  * 	"/hdfs/path/to/folder/of/rate/files", Some(sparkContext),
  * 	aWeekAgo, today, tolerateUnexpectedMissingRateFiles = true
  * )
  * // Even if you don't have the EUR to USD rate for 20170327, but you have it for 20170326, then this will return the
  * // rate / converted price for 20170326. On the contrary, if the EUR to USD rate can't be found for all dates between
  * // 20170320 and 20170327, then this will throw an error:
  * currencyConverter.convertAndFallback(12.5f, "EUR", "USD", "20170327")
  * currencyConverter.getExchangeRateAndFallback("EUR", "SEK", "20170327")
  * }}}
  *
  * * You want to use the currency converter with whatever conditions, but you
  * have a different format of exchange rates than the expected one (a csv with
  * 4 fields: dateOfApplicability,fromCurrency,toCurrency, rate). In this case,
  * you can provide to the constructor of the CurrencyConverter a function which
  * will replace the default one and which parses a rate line under your format
  * to the object [[com.currency_converter.model.ExchangeRate]].
  * None.
  * {{{
  * // The custom function to parse rate lines:
  * val customParseRateLine = (rawRateLine: String) => {
  * 
  * 	val splittedRateLine = rawRateLine.split("\\,", -1) // 2017-02-01,USD,,SEK,,,8.80033
  * 
  * 	val date = splittedRateLine(0).replace("-", "")
  * 	val fromCurrency = splittedRateLine(1)
  * 	val toCurrency = splittedRateLine(3)
  * 	val exchangeRate = splittedRateLine(6).toDouble
  * 
  * 	Some(ExchangeRate(date, fromCurrency, toCurrency, exchangeRate))
  * }
  * // And you give it to the CurrencyConverter constructor:
  * val currencyConverter = new CurrencyConverter(
  * 	"/hdfs/path/to/folder/of/rate/files", Some(sparkContext),
  * 	parseRateLine = customParseRateLine
  * )
  * }}}
  * Notice how the custom parsing function you provide requests to return an
  * Option of ExchangeRate. This allows one to filter lines of rates he consider
  * as invalid by returning None.
  * Notice as well that we don't provide a method, but a function (not "def",
  * but "var"), otherwise, when using the CurrencyConverter with Spark you'll
  * get a Serializable exception.
  *
  * With Spark, don't forget that you can broadcast the CurrencyConverter object.
  *
  * Source <a href="https://github.com/xavierguihot/currency_converter/blob/
  * master/src/main/scala/com/currency_converter/CurrencyConverter.scala">
  * CurrencyConverter</a>
  *
  * @constructor Creates a CurrencyConverter.
  *
  * The expected format of raw exchange rates is:
  * 	yyyyMMddDateOfApplicability,fromCurrency,toCurrency,rate
  * 	20170327,USD,EUR,0.89
  * In case you have a different format, then use the parameter parseRateLine
  * with a function which parses your format of rate line.
  *
  * @param currencyFolder the path to the folder which contains files of
  * exchange rates for all dates (unix path or hdfs path).
  * @param sparkContext the Spark context if you work with Spark or None
  * otherwise.
  * @param firstDateOfRates the first (included) date of the range of dates of
  * exchange rates to load (expected format is yyyyMMdd - for instance 20170327).
  * @param lastDateOfRates (default = "", which means yesterday) the last date
  * (included) of the range of dates of exchange rates to load (expected format
 * is yyyyMMdd - for instance 20170415).
  * @param tolerateUnexpectedMissingRateFiles (default = false) When set to
  * false, if no exchange rates could be loaded for at least one date within the
  * provided range of dates to load, then an exception is thrown at the creation
  * of the CurrencyConverter instance.
  * @param parseRateLine (default is for format "20170327,USD,EUR,0.89") the
  * function used to parse raw exchange rates. If you have the possibility to
  * provide data under the default format, then forget this parameter.
  *
  * @author Xavier Guihot
  * @since 2017-02
  */
class CurrencyConverter(
	currencyFolder: String, sparkContext: Option[SparkContext] = None,
	firstDateOfRates: String = "20140101", lastDateOfRates: String = "",
	tolerateUnexpectedMissingRateFiles: Boolean = false,
	parseRateLine: String => Option[ExchangeRate] = Loader.parseDefaultRateLine
) extends Serializable {

	// This contains all currency->usd exchange rates for the requested range of
	// dates:
	private val toUsdRates = Loader.loadExchangeRates(
		sparkContext, currencyFolder, parseRateLine,
		firstDateOfRates, lastDateOfRates, tolerateUnexpectedMissingRateFiles
	)

	/** Converts a price from currency XXX to YYY.
	  *
	  * {{{
	  * assert(currencyConverter.convert(12.5f, "EUR", "USD", "20170201") == 13.415185f)
	  * }}}
	  *
	  * Throws a CurrencyConverterException if the rate isn't available for the
	  * requested date.
	  *
	  * @param price the price in currency XXX
	  * @param fromCurrency the currency for which the price is given
	  * @param toCurrency the currency in which we want to convert the price
	  * @param forDate the date for which we want the price
	  * @param format (default = "yyyyMMdd") the format under which is provided
	  * the date of the requested exchange rate.
	  * @return the price converted in currency YYY
	  */
	@throws(classOf[CurrencyConverterException])
	def convert(
		price: Double, fromCurrency: String, toCurrency: String, forDate: String,
		format: String = "yyyyMMdd"
	): Double = {
		price * getExchangeRate(fromCurrency, toCurrency, forDate, format)
	}

	/** Returns the exchange rate from currency XXX to YYY.
	  *
	  * {{{
	  * assert(currencyConverter.getExchangeRate("EUR", "SEK", "20170201") == 9.444644f)
	  * }}}
	  *
	  * Throws a CurrencyConverterException if the rate isn't available for the
	  * requested date.
	  *
	  * @param fromCurrency the source currency
	  * @param toCurrency the target currency
	  * @param forDate the date for which we want the exchange rate
	  * @param format (default = "yyyyMMdd") the format under which is provided
	  * the date of the requested exchange rate.
	  * @return the exchange rate from currency XXX to YYY
	  */
	@throws(classOf[CurrencyConverterException])
	def getExchangeRate(
		fromCurrency: String, toCurrency: String, forDate: String,
		format: String = "yyyyMMdd"
	): Double = {

		// This is not an optimization since its rare to apply it (a user
		// usually doesn't need to get the rate from a currency to the same
		// currency) but rather a way to avoid having an exception when a user
		// does require to get this rate of 1 but on a currency for which input
		// data doesn't contain a rate from this currency to usd (indeed rates
		// are stored as currency->usd, so this means in this case that we would
		// apply currency->usd then usd->currency, which would throw an exception):
		if (fromCurrency == toCurrency)
			1f

		else {

			val yyyyMMddDate = reformatDate(forDate, format)

			val sourceCurrencyToUsdRate = getToUsdRate(fromCurrency, yyyyMMddDate)
			val targetCurrencyToUsdRate = getToUsdRate(toCurrency, yyyyMMddDate)

			sourceCurrencyToUsdRate * (1 / targetCurrencyToUsdRate)
		}
	}

	// Fallback aliases:

	/** Converts a price from currency XXX to YYY and if needed and possible, fallback on earlier date.
	  *
	  * If the rate is missing for the requested date, this provides a way to
	  * try and get the conversion from a previous date.
	  *
	  * For instance, if the USD/GBP rate is requested for 20170328, but there
	  * is no data for 20170328, then this method will check if the USD/GBP rate
	  * is available for the previous date (20170327) and use this one instead.
	  * But if it's still not available, this will check for the previous day
	  * again (20170326) and if it's available, this rate will be used. And so
	  * on, up to the  "firstDateOfRates" provided in the CurrencyConverter
	  * constructor.
	  *
	  * {{{
	  * assert(currencyConverter.convertAndFallback(2f, "USD", "GBP", "20170228") == 1.59838f)
	  * // where:
	  * assert(currencyConverter.convertOrElse(2f, "USD", "GBP", "20170228", 2f) == 2f)
	  * assert(currencyConverter.convertOrElse(2f, "USD", "GBP", "20170227", 2f) == 2f)
	  * assert(currencyConverter.convertOrElse(2f, "USD", "GBP", "20170226", 2f) == 1.59838f)
	  * }}}
	  *
	  * Throws a CurrencyConverterException if the rate isn't available for all
	  * dates between the CurrencyConverter parameter firstDateOfRates and the
	  * requested date of conversion.
	  *
	  * @param price the price in currency XXX
	  * @param fromCurrency the currency for which the price is given
	  * @param toCurrency the currency in which we want to convert the price
	  * @param forDate the date for which we want the price
	  * @param format (default = "yyyyMMdd") the format under which is provided
	  * the date of the requested exchange rate.
	  * @return the price converted in currency YYY
	  */
	@throws(classOf[CurrencyConverterException])
	def convertAndFallback(
		price: Double, fromCurrency: String, toCurrency: String, forDate: String,
		format: String = "yyyyMMdd"
	): Double = {
		convertAndFallbackWithDetail(
			price, fromCurrency, toCurrency, forDate, format
		)._1
	}

	/** Converts a price from currency XXX to YYY and if needed and possible, fallback on earlier date.
	  *
	  * If the rate is missing for the requested date, this provides a way to
	  * try and get the conversion from a previous date.
	  *
	  * For instance, if the USD/GBP rate is requested for 20170328, but there
	  * is no data for 20170328, then this method will check if the USD/GBP rate
	  * is available for the previous date (20170327) and use this one instead.
	  * But if it's still not available, this will check for the previous day
	  * again (20170326) and if it's available, this rate will be used. And so
	  * on, up to the  "firstDateOfRates" provided in the CurrencyConverter
	  * constructor.
	  *
	  * This method returns a tuple with the converted price and the date for
	  * which this a rate was available.
	  *
	  * {{{
	  * assert(currencyConverter.convertAndFallbackWithDetail(2f, "USD", "GBP", "20170228") == (1.59838f, "20170226"))
	  * // where:
	  * assert(currencyConverter.convertOrElse(2f, "USD", "GBP", "20170228", 2f) == 2f)
	  * assert(currencyConverter.convertOrElse(2f, "USD", "GBP", "20170227", 2f) == 2f)
	  * assert(currencyConverter.convertOrElse(2f, "USD", "GBP", "20170226", 2f) == 1.59838f)
	  * }}}
	  *
	  * Throws a CurrencyConverterException if the rate isn't available for all
	  * dates between the CurrencyConverter parameter firstDateOfRates and the
	  * requested date of conversion.
	  *
	  * @param price the price in currency XXX
	  * @param fromCurrency the currency for which the price is given
	  * @param toCurrency the currency in which we want to convert the price
	  * @param forDate the date for which we want the price
	  * @param format (default = "yyyyMMdd") the format under which is provided
	  * the date of the requested exchange rate.
	  * @return the tuple (price converted in currency YYY, date used for the
	  * rate).
	  */
	@throws(classOf[CurrencyConverterException])
	def convertAndFallbackWithDetail(
		price: Double, fromCurrency: String, toCurrency: String, forDate: String,
		format: String = "yyyyMMdd"
	): (Double, String) = {

		val exchangeRateWithDetail = getExchangeRateAndFallbackWithDetail(
			fromCurrency, toCurrency, forDate, format
		)

		val exchangeRate = exchangeRateWithDetail._1
		val fallbackDate = exchangeRateWithDetail._2

		(price * exchangeRate, fallbackDate)
	}

	/** Returns the exchange rate from currency XXX to YYY and fallback on earlier date.
	  *
	  * In case the rate is missing for the requested date, this provides a way
	  * to try and get the rate from a previous date.
	  *
	  * For instance, if the USD/GBP rate is requested for 20170328, but there
	  * is no data for 20170328, then this method will check if the USD/GBP rate
	  * is available for the previous date (20170327) and use this one instead.
	  * But if it's still not available, this will check for the previous day
	  * again (20170326) and if it's available, this rate will be used. And so
	  * on, up to the  "firstDateOfRates" provided in the CurrencyConverter
	  * constructor.
	  *
	  * {{{
	  * assert(currencyConverter.getExchangeRateAndFallback("USD", "GBP", "20170228") == 0.9317799f)
	  * // where:
	  * assert(currencyConverter.getExchangeRateOrElse("USD", "GBP", "20170228", 1f) == 1f)
	  * assert(currencyConverter.getExchangeRateOrElse("USD", "GBP", "20170227", 1f) == 1f)
	  * assert(currencyConverter.getExchangeRateOrElse("USD", "GBP", "20170226", 1f) == 0.9317799f)
	  * }}}
	  *
	  * Throws a CurrencyConverterException if the rate isn't available for all
	  * dates between the CurrencyConverter parameter firstDateOfRates and the
	  * requested date of conversion.
	  *
	  * @param fromCurrency the source currency
	  * @param toCurrency the target currency
	  * @param forDate the date for which we want the exchange rate
	  * @param format (default = "yyyyMMdd") the format under which is provided
	  * the date of the requested exchange rate.
	  * @return the exchange rate for the requested date or the earliest
	  * previous dte for which there was available data.
	  */
	@throws(classOf[CurrencyConverterException])
	def getExchangeRateAndFallback(
		fromCurrency: String, toCurrency: String, forDate: String,
		format: String = "yyyyMMdd"
	): Double = {
		getExchangeRateAndFallbackWithDetail(
			fromCurrency, toCurrency, forDate, format
		)._1
	}

	/** Returns the exchange rate from currency XXX to YYY and fallback on earlier date.
	  *
	  * In case the rate is missing for the requested date, this provides a way
	  * to try and get the rate from a previous date.
	  *
	  * For instance, if the USD/GBP rate is requested for 20170328, but there
	  * is no data for 20170328, then this method will check if the USD/GBP rate
	  * is available for the previous date (20170327) and use this one instead.
	  * But if it's still not available, this will check for the previous day
	  * again (20170326) and if it's available, this rate will be used. And so
	  * on, up to the  "firstDateOfRates" provided in the CurrencyConverter
	  * constructor.
	  *
	  * This method returns a tuple with the rate and the date for which this
	  * rate was available.
	  *
	  * {{{
	  * assert(currencyConverter.getExchangeRateAndFallbackWithDetail("USD", "GBP", "20170228") == (0.9317799f, "20170226"))
	  * // where:
	  * assert(currencyConverter.getExchangeRateOrElse("USD", "GBP", "20170228", 1f) == 1f)
	  * assert(currencyConverter.getExchangeRateOrElse("USD", "GBP", "20170227", 1f) == 1f)
	  * assert(currencyConverter.getExchangeRateOrElse("USD", "GBP", "20170226", 1f) == 0.9317799f)
	  * }}}
	  *
	  * Throws a CurrencyConverterException if the rate isn't available for all
	  * dates between the CurrencyConverter parameter firstDateOfRates and the
	  * requested date of conversion.
	  *
	  * @param fromCurrency the source currency
	  * @param toCurrency the target currency
	  * @param forDate the date for which we want the exchange rate
	  * @return a tuple such as (12.5f, "20170324") with the exchange rate from
	  * currency XXX to YYY and the date this currency was for (the requested date if data was available or
	  * a previous date otherwise).
	  * @param format (default = "yyyyMMdd") the format under which is provided
	  * the date of the requested exchange rate.
	  */
	@throws(classOf[CurrencyConverterException])
	def getExchangeRateAndFallbackWithDetail(
		fromCurrency: String, toCurrency: String, forDate: String,
		format: String = "yyyyMMdd"
	): (Double, String) = {

		val yyyyMMddDate = reformatDate(forDate, format)

		val rate = getExchangeRateOrElse(
			fromCurrency, toCurrency, yyyyMMddDate, -1f
		)

		if (rate != -1f)
			(rate, yyyyMMddDate)

		else {

			val formatter = DateTimeFormat.forPattern("yyyyMMdd")

			val dayBefore = formatter.print(
				formatter.parseDateTime(yyyyMMddDate).minusDays(1)
			)

			if (dayBefore < firstDateOfRates)
				throw CurrencyConverterException(
					"No exchange rate for one of the currencies \"" +
					fromCurrency + "\" or \"" + toCurrency + "\" " +
					"and no fallback could be found on previous dates"
				)

			getExchangeRateAndFallbackWithDetail(
				fromCurrency, toCurrency, dayBefore
			)
		}
	}

	// Aliases:

	/** Converts a price from currency XXX to YYY.
	  *
	  * {{{
	  * // If the rate is available for the requested date:
	  * assert(currencyConverter.convertOrElse(12.5f, "EUR", "USD", "20170201", 12.5f) == 13.415185f)
	  * // Otherwise:
	  * assert(currencyConverter.convertOrElse(12.5f, "EUR", "USD", "20170202", 12.5f) == 12.5f)
	  * }}}
	  *
	  * @param price the price in currency XXX
	  * @param fromCurrency the currency for which the price is given
	  * @param toCurrency the currency in which we want to convert the price
	  * @param forDate the date for which we want the price
	  * @param orElse the value to return if an error ocured (missing rate)
	  * @param format (default = "yyyyMMdd") the format under which is provided
	  * the date of the requested exchange rate.
	  * @return the price converted in currency YYY
	  */
	def convertOrElse(
		price: Double, fromCurrency: String, toCurrency: String, forDate: String,
		orElse: Double, format: String = "yyyyMMdd"
	): Double = {
		try {
			convert(price, fromCurrency, toCurrency, forDate, format)
		} catch { case _: CurrencyConverterException => orElse }
	}

	/** Returns the exchange rate from currency XXX to YYY.
	  *
	  * This doesn't throw an exception when the requested rate for the
	  * requested date is missing. Instead, it returns the value given through
	  * the orElse parameter.
	  *
	  * {{{
	  * // If the rate is available for the requested date:
	  * assert(currencyConverter.getExchangeRateOrElse("EUR", "USD", "20170201", 12.5f) == 1.0732148f)
	  * // Otherwise:
	  * assert(currencyConverter.getExchangeRateOrElse("EUR", "USD", "20170202", 1f) == 1f)
	  * }}}
	  *
	  * @param fromCurrency the source currency
	  * @param toCurrency the target currency
	  * @param forDate the date for which we want the exchange rate
	  * @param orElse the value to return if an error ocured (missing rate)
	  * @param format (default = "yyyyMMdd") the format under which is provided
	  * the date of the requested exchange rate.
	  * @return the exchange rate from currency XXX to YYY
	  */
	def getExchangeRateOrElse(
		fromCurrency: String, toCurrency: String, forDate: String, orElse: Double,
		format: String = "yyyyMMdd"
	): Double = {
		try {
			getExchangeRate(fromCurrency, toCurrency, forDate, format)
		} catch { case _: CurrencyConverterException => orElse }
	}

	// Internal core:

	/** Returns the rate from currency XXX to USD.
	  *
	  * @param currency the currency for which we want the exchange rate to USD
	  * @param date the user input date
	  * @return the rate from the currency to USD
	  */
	private def getToUsdRate(currency: String, date: String): Double = {

		// Since the USD to USD rate is not provided in input data:
		if (currency == "USD")
			1f

		else {

			// If for the given date we have the rate:
			if (
				!toUsdRates.contains(date) ||
				!toUsdRates(date).contains(currency)
			)
				// If we get there, then we throw an exception:
				throw CurrencyConverterException(
					"No exchange rate for currency \"" + currency + "\" " +
					"for date \"" + date + "\"."
				)

			toUsdRates(date)(currency)
		}
	}

	private def reformatDate(date: String, inputFormat: String): String = {
		try {
			DateTimeFormat.forPattern("yyyyMMdd").print(
				DateTimeFormat.forPattern(inputFormat).parseDateTime(date)
			)
		} catch {
			case iae: IllegalArgumentException => {
				throw new IllegalArgumentException(
					"Date \"" + date + "\" doesn't look like a " +
					inputFormat + " date."
				)
			}
		}
	}
}
