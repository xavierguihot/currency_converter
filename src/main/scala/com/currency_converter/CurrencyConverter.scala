package com.currency_converter

import com.currency_converter.error.CurrencyConverterException
import com.currency_converter.load.Loader
import com.currency_converter.model.ExchangeRate

import org.apache.spark.SparkContext

import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, Days}

import scala.util.{Try, Success, Failure}

/** A '''Currency Converter'''.
  *
  * A Scala Wrapper around your exchange rate data for currency conversion.
  *
  * Based on '''your exchange rate files''' stored either on a classic '''file
  * system''' or on '''HDFS''' (Hadoop), this CurrencyConverter object provides
  * for both classic and Spark jobs methods to '''convert prices''' and
  * '''retrieve exchange rates'''.
  *
  * * Usually, one will use the CurrencyConverter this way:
  *
  * {{{
  * import com.currency_converter.CurrencyConverter
  * val currencyConverter = new CurrencyConverter("/path/to/folder/of/rate/files")
  * // Or when data is stored on Hadoop:
  * val currencyConverter = new CurrencyConverter("/hdfs/path/to/folder/of/rate/files", sparkContext)
  * // And then, to get the exchange rate and the converted price from EUR to SEK for the date 20170201:
  * currencyConverter.exchangeRate("EUR", "SEK", "20170201")
  * currencyConverter.convert(12.5d, "EUR", "USD", "20170201")
  * }}}
  *
  * * It's often the case that one doesn't need to have the exact exchange rate of
  * the requested date if the rate isn't available for this date. In this case,
  * the following methods give the possibility to fallback on the rate of
  * previous dates when it's not available for the given date:
  *
  * {{{
  * // if:
  * assert(currencyConverter.exchangeRate("USD", "GBP", "20170228").isFailure)
  * assert(currencyConverter.exchangeRate("USD", "GBP", "20170227").isFailure)
  * assert(currencyConverter.exchangeRate("USD", "GBP", "20170226") == Success(0.9317799d))
  * // then:
  * assert(currencyConverter.getExchangeRateAndFallBack("USD", "GBP", "20170228") == Success(0.9317799d))
  * assert(currencyConverter.convertAndFallBack(2d, "USD", "GBP", "20170228") == Success(1.59838d))
  * }}}
  *
  * * To load exchange rate data, this tool expects your exchange rate data to
  * be csv formated this way:
  *
  * 	yyyyMMddDateOfApplicability,fromCurrency,toCurrency,rate (20170327,USD,EUR,0.89)
  *
  * But if it's not the case, you can provide a custom exchange rate line
  * parser, such as:
  *
  * {{{
  * import com.currency_converter.model.ExchangeRate
  * // For instance, for a custom format such as: 2017-02-01,USD,,EUR,,,0.93178:
  * val customRateLineParser = (rawRateLine: String) => {
  *
  * 	val splittedRateLine = rawRateLine.split("\\,", -1)
  *
  * 	val date = splittedRateLine(0).replace("-", "")
  * 	val fromCurrency = splittedRateLine(1)
  * 	val toCurrency = splittedRateLine(3)
  * 	val exchangeRate = splittedRateLine(6).toFloat
  *
  * 	Some(ExchangeRate(date, fromCurrency, toCurrency, exchangeRate))
  * }
  * }}}
  *
  * * Finally, you can request a specific range of dates for the rates to load.
  * Indeed, the default dates to load are 20140101 to today. This might be
  * either too restrictive or you might want to load less data due to very
  * limited available memory.
  *
  * * With Spark, don't forget that you can broadcast the CurrencyConverter
  * object.
  *
  * Source <a href="https://github.com/xavierguihot/currency_converter/blob/
  * master/src/main/scala/com/currency_converter/CurrencyConverter.scala">
  * CurrencyConverter</a>
  *
  * @author Xavier Guihot
  * @since 2017-02
  */
class CurrencyConverter private (
	currencyFolder: String, sparkContext: Option[SparkContext] = None,
	firstDateOfRates: String = "20140101",
	lastDateOfRates: String = CurrencyConverter.today,
	rateLineParser: String => Option[ExchangeRate] = ExchangeRate.defaultRateLineParser
) extends Serializable {

	// This contains all currency->usd exchange rates for the requested range of
	// dates:
	private val toUsdRates: Map[String, Map[String, Double]] =
		Loader.loadExchangeRates(
			currencyFolder, sparkContext, firstDateOfRates, lastDateOfRates,
			rateLineParser
		)

	/** Converts a price from currency XXX to YYY.
	  *
	  * {{{
	  * assert(currencyConverter.convert(12.5d, "EUR", "USD", "20170201") == Success(13.41d))
	  * assert(currencyConverter.convert(12.5d, "EUR", "?#~", "20170201") == Failure(CurrencyConverterException("No exchange rate for currency \"?#~\" for date \"20170201\".")))
	  * }}}
	  *
	  * @param price the price in currency XXX
	  * @param fromCurrency the currency for which the price is given
	  * @param toCurrency the currency in which we want to convert the price
	  * @param forDate the date for which we want the price
	  * @param format (default = "yyyyMMdd") the format under which is provided
	  * the date of the requested exchange rate.
	  * @return the price converted in currency YYY
	  */
	def convert(
		price: Double, fromCurrency: String, toCurrency: String, forDate: String,
		format: String = "yyyyMMdd"
	): Try[Double] = {

		exchangeRate(fromCurrency, toCurrency, forDate, format) match {
			case Success(rate)      => Success(price * rate)
			case Failure(exception) => Failure(exception)
		}
	}

	/** Returns the exchange rate from currency XXX to YYY.
	  *
	  * {{{
	  * assert(currencyConverter.exchangeRate("EUR", "SEK", "20170201") == Success(9.44d))
	  * assert(currencyConverter.exchangeRate("EUR", "?#~", "20170201") == Failure(CurrencyConverterException("No exchange rate for currency \"?#~\" for date \"20170201\".")))
	  * }}}
	  *
	  * @param fromCurrency the source currency
	  * @param toCurrency the target currency
	  * @param forDate the date for which we want the exchange rate
	  * @param format (default = "yyyyMMdd") the format under which is provided
	  * the date of the requested exchange rate.
	  * @return the exchange rate from currency XXX to YYY
	  */
	def exchangeRate(
		fromCurrency: String, toCurrency: String, forDate: String,
		format: String = "yyyyMMdd"
	): Try[Double] = {

		// This is not an optimization since its rare to apply it (a user
		// usually doesn't need to get the rate from a currency to the same
		// currency) but rather a way to avoid having an exception when a user
		// does require to get this rate of 1 but on a currency for which input
		// data doesn't contain a rate from this currency to usd (indeed rates
		// are stored as currency->usd, so this means in this case that we would
		// apply currency->usd then usd->currency, which would throw an exception):
		if (fromCurrency == toCurrency)
			Success(1d)

		else {

			val date = CurrencyConverter.yyyyMMddDate(forDate, format)

			val sourceCurrencyToUsdRate = getToUsdRate(fromCurrency, date)
			val targetCurrencyToUsdRate = getToUsdRate(toCurrency, date)

			(sourceCurrencyToUsdRate, targetCurrencyToUsdRate) match {

				case (Success(sourceCurrencyToUsdRate), Success(targetCurrencyToUsdRate)) =>
					Success(sourceCurrencyToUsdRate * (1d / targetCurrencyToUsdRate))

				case (Failure(exception), _) => Failure(exception)

				case (_, Failure(exception)) => Failure(exception)
			}
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
	  * Even if this is supposed to return a fall back value based on rates of
	  * previous dates, the return type is embedded in a Try. Indeed, it might
	  * happen that even when using older rates, no rate could be available.
	  *
	  * {{{
	  * // if:
	  * assert(currencyConverter.convert(2d, "USD", "GBP", "20170228").isFailure)
	  * assert(currencyConverter.convert(2d, "USD", "GBP", "20170227").isFailure)
	  * assert(currencyConverter.convert(2d, "USD", "GBP", "20170226") == Success(1.59838d))
	  * // then:
	  * assert(currencyConverter.convertAndFallBack(2d, "USD", "GBP", "20170228") == Success(1.59838d))
	  * }}}
	  *
	  * @param price the price in currency XXX
	  * @param fromCurrency the currency for which the price is given
	  * @param toCurrency the currency in which we want to convert the price
	  * @param forDate the date for which we want the price
	  * @param format (default = "yyyyMMdd") the format under which is provided
	  * the date of the requested exchange rate.
	  * @return the price converted in currency YYY
	  */
	def convertAndFallBack(
		price: Double, fromCurrency: String, toCurrency: String, forDate: String,
		format: String = "yyyyMMdd"
	): Try[Double] = {

		val convertedPriceWithDetail = convertAndFallBackWithDetail(
			price, fromCurrency, toCurrency, forDate, format
		)

		convertedPriceWithDetail match {
			case Success((convertedPrice, fallBackDate)) => Success(convertedPrice)
			case Failure(exception)                      => Failure(exception)
		}
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
	  * Even if this is supposed to return a fall back value based on rates of
	  * previous dates, the return type is embedded in a Try. Indeed, it might
	  * happen that even when using older rates, no rate could be available.
	  *
	  * {{{
	  * // if:
	  * assert(currencyConverter.convert(2d, "USD", "GBP", "20170228").isFailure)
	  * assert(currencyConverter.convert(2d, "USD", "GBP", "20170227").isFailure)
	  * assert(currencyConverter.convert(2d, "USD", "GBP", "20170226") == Success(1.59838d))
	  * // then:
	  * assert(currencyConverter.convertAndFallBackWithDetail(2d, "USD", "GBP", "20170228") == Success((1.59838d, "20170226")))
	  * }}}
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
	def convertAndFallBackWithDetail(
		price: Double, fromCurrency: String, toCurrency: String, forDate: String,
		format: String = "yyyyMMdd"
	): Try[(Double, String)] = {

		val exchangeRateWithDetail = getExchangeRateAndFallBackWithDetail(
			fromCurrency, toCurrency, forDate, format
		)

		exchangeRateWithDetail match {
			case Success((exchangeRate, fallBackDate)) => Success((price * exchangeRate, fallBackDate))
			case Failure(exception) => Failure(exception)
		}
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
	  * Even if this is supposed to return a fall back value based on rates of
	  * previous dates, the return type is embedded in a Try. Indeed, it might
	  * happen that even when using older rates, no rate could be available.
	  *
	  * {{{
	  * // if:
	  * assert(currencyConverter.exchangeRate("USD", "GBP", "20170228").isFailure)
	  * assert(currencyConverter.exchangeRate("USD", "GBP", "20170227").isFailure)
	  * assert(currencyConverter.exchangeRate("USD", "GBP", "20170226") == Success(0.9317799d))
	  * // then:
	  * assert(currencyConverter.getExchangeRateAndFallBack("USD", "GBP", "20170228") == Success(0.9317799d))
	  * }}}
	  *
	  * @param fromCurrency the source currency
	  * @param toCurrency the target currency
	  * @param forDate the date for which we want the exchange rate
	  * @param format (default = "yyyyMMdd") the format under which is provided
	  * the date of the requested exchange rate.
	  * @return the exchange rate for the requested date or the earliest
	  * previous dte for which there was available data.
	  */
	def getExchangeRateAndFallBack(
		fromCurrency: String, toCurrency: String, forDate: String,
		format: String = "yyyyMMdd"
	): Try[Double] = {

		val rateWithDetail = getExchangeRateAndFallBackWithDetail(
			fromCurrency, toCurrency, forDate, format
		)

		rateWithDetail match {
			case Success((rate, fallBackDate)) => Success(rate)
			case Failure(exception)            => Failure(exception)
		}
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
	  * Even if this is supposed to return a fall back value based on rates of
	  * previous dates, the return type is embedded in a Try. Indeed, it might
	  * happen that even when using older rates, no rate could be available.
	  *
	  * {{{
	  * // if:
	  * assert(currencyConverter.exchangeRate("USD", "GBP", "20170228").isFailure)
	  * assert(currencyConverter.exchangeRate("USD", "GBP", "20170227").isFailure)
	  * assert(currencyConverter.exchangeRate("USD", "GBP", "20170226") == Success(0.9317799d))
	  * // then:
	  * assert(currencyConverter.getExchangeRateAndFallBackWithDetail("USD", "GBP", "20170228") == Succcess((0.9317799d, "20170226")))
	  * }}}
	  *
	  * @param fromCurrency the source currency
	  * @param toCurrency the target currency
	  * @param forDate the date for which we want the exchange rate
	  * @return a tuple such as (12.5d, "20170324") with the exchange rate from
	  * currency XXX to YYY and the date this currency was for (the requested
	  * date if data was available or a previous date otherwise).
	  * @param format (default = "yyyyMMdd") the format under which is provided
	  * the date of the requested exchange rate.
	  */
	def getExchangeRateAndFallBackWithDetail(
		fromCurrency: String, toCurrency: String, forDate: String, format: String = "yyyyMMdd"
	): Try[(Double, String)] = {

		val date = CurrencyConverter.yyyyMMddDate(forDate, format)

		exchangeRate(fromCurrency, toCurrency, date) match {

			case Success(rate) => Success((rate, date))

			case Failure(exception) => {

				val formatter = DateTimeFormat.forPattern("yyyyMMdd")

				val dayBefore = formatter.print(
					formatter.parseDateTime(date).minusDays(1)
				)

				if (dayBefore >= firstDateOfRates)
					getExchangeRateAndFallBackWithDetail(fromCurrency, toCurrency, dayBefore)
				else
					Failure(CurrencyConverterException(
						"No exchange rate between currencies \"" +
						fromCurrency + "\" and \"" + toCurrency + "\" " +
						"could be found even after fallback on previous dates."
					))
			}
		}
	}

	/** Checks that all dates within the required range have at least one rate.
	  *
	  * Depending on the use case, this might not matter, specially with fall
	  * back methods.
	  *
	  * This finds out if all dates requested when creating this
	  * CurrencyConverter (dates within the range [firstDateOfRates, lastDateOfRates])
	  * have at least one exchange rate after loading data. Since usually rates
	  * data is provided one date per by file, this gives the user the
	  * possibility to know and take actions if one date has no rates.
	  *
	  * @return if all dates within the required range have at least one rate
	  */
	def allDatesHaveRates(): Boolean = {

		val dateFormatter = DateTimeFormat.forPattern("yyyyMMdd")

		val startDate = dateFormatter.parseDateTime(firstDateOfRates)
		val lastDate = dateFormatter.parseDateTime(lastDateOfRates)

		val missingDates = (
			0 to Days.daysBetween(startDate, lastDate).getDays()
		).toList.map(
			dayNbr => dateFormatter.print(startDate.plusDays(dayNbr))
		).filter(
			date => !toUsdRates.contains(date)
		)

		missingDates.isEmpty
	}

	// Internal core:

	/** Returns the rate from currency XXX to USD.
	  *
	  * @param currency the currency for which we want the exchange rate to USD
	  * @param date the user input date under format "yyyyMMdd"
	  * @return the rate from the currency to USD
	  */
	private def getToUsdRate(currency: String, date: String): Try[Double] = {

		currency match {

			case "USD" => Success(1d) // Since the USD to USD rate is not provided in input data

			case _ => {

				toUsdRates.get(date) match {

					case Some(toUsdRatesForDate) => toUsdRatesForDate.get(currency) match {
						case Some(currencyToUsdRate) => Success(currencyToUsdRate)
						case None => Failure(CurrencyConverterException(
							"No exchange rate for currency \"" + currency +
							"\" for date \"" + date + "\"."
						))
					}

					case None => Failure(CurrencyConverterException(
						"No exchange rate for date \"" + date + "\"."
					))
				}
			}
		}
	}

	// Constructors:

	/** Creates a CurrencyConverter.
	  *
	  * @param currencyFolder the path to the hdfs folder which contains the
	  * files of exchange rates.
	  */
	def this(currencyFolder: String) { this(currencyFolder, None) }

	/** Creates a CurrencyConverter with a specific range of dates.
	  *
	  * @param currencyFolder the path to the hdfs folder which contains the
	  * files of exchange rates.
	  * @param firstDateOfRates the first (included) date of the range of dates
	  * of exchange rates to load (expected format is yyyyMMdd - for instance
	  * 20170327).
	  * @param lastDateOfRates the last date (included) date of the range of
	  * dates of exchange rates to load (expected format is yyyyMMdd - for
	  * instance 20170415).
	  */
	def this(currencyFolder: String, firstDateOfRates: String, lastDateOfRates: String) {
		this(currencyFolder, None, firstDateOfRates, lastDateOfRates)
	}

	/** Creates a CurrencyConverter for data stored under a different format
	  * than the specified one.
	  *
	  * @param currencyFolder the path to the hdfs folder which contains the
	  * files of exchange rates.
	  * @param rateLineParser the custom function used to parse raw exchange
	  * rates. If you have the possibility to provide data under the default
	  * format, then forget this parameter.
	  */
	def this(currencyFolder: String, rateLineParser: String => Option[ExchangeRate]) {
		this(currencyFolder, None, rateLineParser = rateLineParser)
	}

	/** Creates a CurrencyConverter, with a specific range of dates and for data
	  * stored under a different format than the specified one.
	  *
	  * @param currencyFolder the path to the hdfs folder which contains the
	  * files of exchange rates.
	  * @param firstDateOfRates the first (included) date of the range of dates
	  * of exchange rates to load (expected format is yyyyMMdd - for instance
	  * 20170327).
	  * @param lastDateOfRates the last date (included) date of the range of
	  * dates of exchange rates to load (expected format is yyyyMMdd - for
	  * instance 20170415).
	  * @param rateLineParser the custom function used to parse raw exchange
	  * rates. If you have the possibility to provide data under the default
	  * format, then forget this parameter.
	  */
	def this(currencyFolder: String, firstDateOfRates: String, lastDateOfRates: String,
			rateLineParser: String => Option[ExchangeRate]) {
		this(currencyFolder, None, firstDateOfRates, lastDateOfRates, rateLineParser)
	}

	/** Creates a CurrencyConverter for data stored on HDFS.
	  *
	  * @param currencyFolder the path to the hdfs folder which contains the
	  * files of exchange rates.
	  * @param sparkContext the SparkContext
	  */
	def this(currencyFolder: String, sparkContext: SparkContext) {
		this(currencyFolder, Some(sparkContext))
	}

	/** Creates a CurrencyConverter for data stored on HDFS, with a specific
	  * range of dates.
	  *
	  * @param currencyFolder the path to the hdfs folder which contains the
	  * files of exchange rates.
	  * @param sparkContext the SparkContext
	  * @param firstDateOfRates the first (included) date of the range of dates
	  * of exchange rates to load (expected format is yyyyMMdd - for instance
	  * 20170327).
	  * @param lastDateOfRates the last date (included) date of the range of
	  * dates of exchange rates to load (expected format is yyyyMMdd - for
	  * instance 20170415).
	  */
	def this(currencyFolder: String, sparkContext: SparkContext,
			firstDateOfRates: String, lastDateOfRates: String) {
		this(currencyFolder, Some(sparkContext), firstDateOfRates, lastDateOfRates)
	}

	/** Creates a CurrencyConverter for data stored on HDFS, for data stored
	  * under a different format than the specified one.
	  *
	  * @param currencyFolder the path to the hdfs folder which contains the
	  * files of exchange rates.
	  * @param sparkContext the SparkContext
	  * @param rateLineParser the custom function used to parse raw exchange
	  * rates. If you have the possibility to provide data under the default
	  * format, then forget this parameter.
	  */
	def this(currencyFolder: String, sparkContext: SparkContext,
			rateLineParser: String => Option[ExchangeRate]) {
		this(currencyFolder, Some(sparkContext), rateLineParser = rateLineParser)
	}

	/** Creates a CurrencyConverter for data stored on HDFS, with a specific
	  * range of dates and for data stored under a different format than the
	  * specified one.
	  *
	  * @param currencyFolder the path to the hdfs folder which contains the
	  * files of exchange rates.
	  * @param sparkContext the SparkContext
	  * @param firstDateOfRates the first (included) date of the range of dates
	  * of exchange rates to load (expected format is yyyyMMdd - for instance
	  * 20170327).
	  * @param lastDateOfRates the last date (included) date of the range of
	  * dates of exchange rates to load (expected format is yyyyMMdd - for
	  * instance 20170415).
	  * @param rateLineParser the custom function used to parse raw exchange
	  * rates. If you have the possibility to provide data under the default
	  * format, then forget this parameter.
	  */
	def this(currencyFolder: String, sparkContext: SparkContext, firstDateOfRates: String,
			lastDateOfRates: String, rateLineParser: String => Option[ExchangeRate]) {
		this(currencyFolder, Some(sparkContext), firstDateOfRates, lastDateOfRates, rateLineParser)
	}
}

private object CurrencyConverter {

	/** We do not catch errors here. User should be owner of its inputs and
	  * should be aware in case the date or format provided gives an exception */
	private def yyyyMMddDate(date: String, inputFormat: String): String = {
		DateTimeFormat.forPattern("yyyyMMdd").print(
			DateTimeFormat.forPattern(inputFormat).parseDateTime(date)
		)
	}

	/** Retrieve today's date */
	private def today(): String = {
		DateTimeFormat.forPattern("yyyyMMdd").print(new DateTime())
	}
}
