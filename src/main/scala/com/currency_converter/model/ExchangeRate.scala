package com.currency_converter.model

/** The representation of an exchange rate.
  *
  * @author Xavier Guihot
  * @since 2017-06
  */
case class ExchangeRate(
	date: String, fromCurrency: String, toCurrency: String, rate: Double
) extends Serializable
