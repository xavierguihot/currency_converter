package com.currency_converter.model

import scala.util.Try

/** The representation of an exchange rate.
  *
  * @author Xavier Guihot
  * @since 2017-06
  */
final case class ExchangeRate(
    date: String,
    fromCurrency: String,
    toCurrency: String,
    rate: Double
) extends Serializable

object ExchangeRate {

  /** Default parsing of an exchange rate line.
    *
    * If one doesn't provide an alternative method to this one when creating
    * the CurrencyConverter object, then this is how lline of rates are parsed.
    *
    * The default format is:
    *
    *   yyyyMMddDateOfApplicability,fromCurrency,toCurrency,rate
    *
    *   20170327,USD,EUR,0.89
    *
    * This returns an Option since users willing to have their custom rate line
    * adapter (to their own rate line format) might want to discard invalid rate
    * lines, which would thus be filtered by the flatMap calling this function.
    *
    * @param rawRateLine the raw rate to parse, such as:
    *   "20170327,USD,EUR,0.89"
    * @return the parsed rate as an ExchangeRate, such as:
    *   Some(ExchangeRate("20170327", "USD", "EUR", 0.89d))
    */
  def defaultRateLineParser(rawRateLine: String): Option[ExchangeRate] =
    rawRateLine.split("\\,", -1) match {
      case Array(date, fromCurrency, toCurrency, exchangeRate) =>
        Try(
          ExchangeRate(date, fromCurrency, toCurrency, exchangeRate.toDouble)
        ).toOption
      case _ => None
    }
}
