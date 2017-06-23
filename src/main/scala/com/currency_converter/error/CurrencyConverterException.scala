package com.currency_converter.error

/** The CurrencyConverter Exception */
case class CurrencyConverterException private[currency_converter] (message: String)
	extends Exception(message)
