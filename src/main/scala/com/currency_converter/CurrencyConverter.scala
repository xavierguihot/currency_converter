package com.currency_converter

import com.currency_converter.error.{CurrencyConverterException => CCE}
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
  * val currencyConverter = new CurrencyConverter(
  *   "/hdfs/path/to/folder/of/rate/files", sparkContext)
  * // And then, to get the exchange rate and the converted price from EUR to
  * // SEK for the date 20170201:
  * currencyConverter.exchangeRate("EUR", "SEK", "20170201")
  * currencyConverter.convert(12.5d, "EUR", "USD", "20170201")
  * }}}
  *
  * * It's often the case that one doesn't need to have the exact exchange rate
  * of the requested date if the rate isn't available for this date. In this
  * case, one case use the fallback option in order to fallback on the rate of
  * previous dates when it's not available for the given date:
  *
  * {{{
  * // if:
  * assert(currencyConverter.exchangeRate("USD", "GBP", "20170228").isFailure)
  * assert(currencyConverter.exchangeRate("USD", "GBP", "20170227").isFailure)
  * assert(currencyConverter.exchangeRate("USD", "GBP", "20170226") == Success(0.93d))
  * // then:
  * assert(currencyConverter.exchangeRate("USD", "GBP", "20170228", fallback = true) == Success(0.93d))
  * assert(currencyConverter.convert(2d, "USD", "GBP", "20170228", fallback = true) == Success(1.59d))
  * }}}
  *
  * * To load exchange rate data, this tool expects your exchange rate data to
  * be csv formatted this way:
  *
  *   yyyyMMddDateOfApplicability,fromCurrency,toCurrency,rate (20170327,USD,EUR,0.89)
  *
  * But if it's not the case, you can provide a custom exchange rate line
  * parser, such as:
  *
  * {{{
  * import com.currency_converter.model.ExchangeRate
  * // For instance, for a custom format such as: 2017-02-01,USD,,EUR,,,0.93178:
  * val customRateLineParser = (rawRateLine: String) => rawRateLine.split("\\,", -1) match {
  *
  *   case Array(date, fromCurrency, toCurrency, exchangeRate) => for {
  *     exchangeRate <- Try(exchangeRate.toDouble).toOption
  *     yyyyMMddDate <- Try(DateTimeFormat
  *       .forPattern("yyyyMMdd")
  *       .print(DateTimeFormat.forPattern("yyyy-MM-dd").parseDateTime(date))).toOption
  *   } yield ExchangeRate(yyyyMMddDate, fromCurrency, toCurrency, exchangeRate)
  *
  *   case _ => None
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
object CurrencyConverter extends Serializable {

  private var firstDateOfRates: String = "20140101"
  private var lastDateOfRates: String = CurrencyConverter.today
  private var lineParser: String => Option[ExchangeRate] =
    ExchangeRate.defaultRateLineParser

  private var toUsdRates: Map[String, Map[String, Double]] = Map()

  def loadFromSpark(currencyFolder: String, sc: SparkContext): Unit =
    toUsdRates = Loader.loadExchangeRates(
      currencyFolder,
      Some(sc),
      firstDateOfRates,
      lastDateOfRates,
      lineParser
    )

  def load(currencyFolder: String): Unit =
    toUsdRates = Loader.loadExchangeRates(
      currencyFolder,
      None,
      firstDateOfRates,
      lastDateOfRates,
      lineParser
    )

  def setFirstDate(firstDate: String): Unit = firstDateOfRates = firstDate

  def setLastDate(lastDate: String): Unit = lastDateOfRates = lastDate

  def setLineParser(rateLineParser: String => Option[ExchangeRate]): Unit =
    lineParser = rateLineParser

  /** Converts a price from currency XXX to YYY.
    *
    * {{{
    * assert(currencyConverter.convert(12.5d, "EUR", "USD", "20170201") == Success(13.41d))
    * assert(currencyConverter.convert(12.5d, "EUR", "USD", "170201", "yyMMdd") == Success(13.41d))
    * assert(currencyConverter.convert(12.5d, "EUR", "?#~", "20170201")
    *   == Failure(CurrencyConverterException("No exchange rate for currency \"?#~\" for date \"20170201\".")))
    * }}}
    *
    * In case the rate to use for the conversion is missing for the requested
    * date, and you still prefer to get a conversion for an older rate from a
    * previous date than a Failure, then you can use the fallback option.
    *
    * For instance, if you request a conversion for the USD/GBP rate for
    * 20170328, but there is no data for 20170328, then this method will check
    * if the USD/GBP rate is available for the previous date (20170327) and use
    * this one instead. But if it's still not available, this will check for the
    * previous day again (20170326) and if it's available, this rate will be
    * used. And so on, up to the "firstDateOfRates" provided in the
    * CurrencyConverter constructor.
    *
    * {{{
    * // if:
    * assert(currencyConverter.convert(2d, "USD", "GBP", "20170228").isFailure)
    * assert(currencyConverter.convert(2d, "USD", "GBP", "20170227").isFailure)
    * assert(currencyConverter.convert(2d, "USD", "GBP", "20170226") == Success(1.59d))
    * // then:
    * assert(currencyConverter.convertAndFallBack(2d, "USD", "GBP", "20170228", fallback = true) == Success(1.59d))
    * }}}
    *
    * @param price the price in currency XXX
    * @param fromCurrency the currency for which the price is given
    * @param toCurrency the currency in which we want to convert the price
    * @param forDate the date for which we want the price
    * @param format (default = "yyyyMMdd") the format under which is provided
    * the date of the requested exchange rate.
    * @param fallback (default false) whether to go back and find the rate in
    * the past if the rate is not available for the conversion for the requested
    * date.
    * @return the price converted in currency YYY
    */
  def convert(
      price: Double,
      fromCurrency: String,
      toCurrency: String,
      forDate: String,
      format: String = "yyyyMMdd",
      fallback: Boolean = false
  ): Try[Double] =
    exchangeRate(fromCurrency, toCurrency, forDate, format, fallback)
      .map(_ * price)

  /** Returns the exchange rate from currency XXX to YYY.
    *
    * {{{
    * assert(currencyConverter.exchangeRate("EUR", "SEK", "20170201") == Success(9.44d))
    * assert(currencyConverter.exchangeRate("EUR", "SEK", "170201", "yyMMdd") == Success(9.44d))
    * assert(currencyConverter.exchangeRate("EUR", "?#~", "20170201") ==
    *   Failure(CurrencyConverterException("No exchange rate for currency \"?#~\" for date \"20170201\".")))
    * }}}
    *
    * In case the rate is missing for the requested date, and you still prefer
    * to get an older rate from a previous date than a Failure, then you can use
    * the fallback option.
    *
    * For instance, if you request the USD/GBP rate for 20170328, but there is
    * no data for 20170328, then this method will check if the USD/GBP rate is
    * available for the previous date (20170327) and use this one instead. But
    * if it's still not available, this will check for the previous day again
    * (20170326) and if it's available, this rate will be used. And so on, up to
    * the "firstDateOfRates" provided in the CurrencyConverter constructor.
    *
    * {{{
    * // if:
    * assert(currencyConverter.exchangeRate("USD", "GBP", "20170228").isFailure)
    * assert(currencyConverter.exchangeRate("USD", "GBP", "20170227").isFailure)
    * assert(currencyConverter.exchangeRate("USD", "GBP", "20170226") == Success(0.93d))
    * // then:
    * assert(currencyConverter.exchangeRate("USD", "GBP", "20170228", fallback = true) == Success(0.93d))
    * }}}
    *
    * @param fromCurrency the source currency
    * @param toCurrency the target currency
    * @param forDate the date for which we want the exchange rate
    * @param format (default = "yyyyMMdd") the format under which is provided
    * the date of the requested exchange rate.
    * @param fallback (default false) whether to go back and find the rate in
    * the past if the rate is not available for the requested date.
    * @return the exchange rate from currency XXX to YYY
    */
  def exchangeRate(
      fromCurrency: String,
      toCurrency: String,
      forDate: String,
      format: String = "yyyyMMdd",
      fallback: Boolean = false
  ): Try[Double] = {

    // This is not an optimization since its rare to apply it (a user usually
    // doesn't need to get the rate from a currency to the same currency) but
    // rather a way to avoid having an exception when a user does require to get
    // this rate of 1 but on a currency for which input data doesn't contain a
    // rate from this currency to usd (indeed rates are stored as currency->usd,
    // so this means in this case that we would apply currency->usd then
    // usd->currency, which would throw an exception):
    if (fromCurrency == toCurrency)
      Success(1d)
    else {

      val date = CurrencyConverter.yyyyMMddDate(forDate, format)

      val rate = for {
        sourceCurrencyToUsdRate <- getToUsdRate(fromCurrency, date)
        targetCurrencyToUsdRate <- getToUsdRate(toCurrency, date)
      } yield sourceCurrencyToUsdRate * (1d / targetCurrencyToUsdRate)

      if (fallback)
        rate.recoverWith {
          case _ =>
            val formatter = DateTimeFormat.forPattern("yyyyMMdd")
            val dayBefore =
              formatter.print(formatter.parseDateTime(date).minusDays(1))
            if (dayBefore >= firstDateOfRates)
              exchangeRate(fromCurrency, toCurrency, dayBefore, fallback = true)
            else
              Failure(
                CCE(
                  "No exchange rate between currencies \"" + fromCurrency +
                    "\" and \"" + toCurrency + "\" could be found even after " +
                    "fallback on previous dates."))
        }
      else
        rate
    }
  }

  /** Checks that all dates within the required range have at least one rate.
    *
    * Depending on the use case, this might not matter, specially with fall back
    * methods.
    *
    * This finds out if all dates requested when creating this CurrencyConverter
    * (dates within the range [firstDateOfRates, lastDateOfRates]) have at least
    * one exchange rate after loading data. Since usually rates data is provided
    * one date per by file, this gives the user the possibility to know and take
    * actions if one date has no rates.
    *
    * @return if all dates within the required range have at least one rate
    */
  def allDatesHaveRates(): Boolean = {

    requiresLoad

    val dateFormatter = DateTimeFormat.forPattern("yyyyMMdd")

    val startDate = dateFormatter.parseDateTime(firstDateOfRates)
    val lastDate = dateFormatter.parseDateTime(lastDateOfRates)

    (0 to Days.daysBetween(startDate, lastDate).getDays).toList
      .map(dayNbr => dateFormatter.print(startDate.plusDays(dayNbr)))
      .forall(date => toUsdRates.contains(date))
  }

  // Internal core:

  /** Returns the rate from currency XXX to USD.
    *
    * @param currency the currency for which we want the exchange rate to USD
    * @param date the user input date under format "yyyyMMdd"
    * @return the rate from the currency to USD
    */
  private def getToUsdRate(currency: String, date: String): Try[Double] = {

    requiresLoad

    currency match {

      case "USD" =>
        Success(1d) // Since the USD to USD rate is not provided in input data

      case _ =>
        toUsdRates.get(date) match {

          case Some(ratesForDate) =>
            ratesForDate
              .get(currency)
              .map(Success(_))
              .getOrElse(
                Failure(CCE("No exchange rate for currency \"" + currency +
                  "\" for date \"" + date + "\".")))

          case None =>
            Failure(CCE("No exchange rate for date \"" + date + "\"."))
        }
    }
  }

  /** We do not catch errors here. User should be owner of its inputs and should
    * be aware in case the date or format provided gives an exception */
  private def yyyyMMddDate(date: String, inputFormat: String): String =
    DateTimeFormat
      .forPattern("yyyyMMdd")
      .print(DateTimeFormat.forPattern(inputFormat).parseDateTime(date))

  /** Retrieve today's date */
  private def today: String =
    DateTimeFormat.forPattern("yyyyMMdd").print(new DateTime())

  private def requiresLoad: Unit = {
    require(
      toUsdRates.nonEmpty,
      "exchange rates should be loaded first using either " +
        "CurrencyConverter.load(\"currency/folder\") or " +
        "CurrencyConverter.loadFromSpark(\"currency/folder\", sc)"
    )
  }
}
