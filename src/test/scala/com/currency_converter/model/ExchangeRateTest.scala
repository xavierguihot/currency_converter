package com.currency_converter.model

import org.scalatest.FunSuite

/** Testing facility for the parsing of exchange rate lines.
  *
  * @author Xavier Guihot
  * @since 2018-01
  */
class ExchangeRateTest extends FunSuite {

	test("Parse OneRate Line") {

		// 1:
		var rawRate = "20171224,SEK,USD,8.33034829"
		var expectedRate = Some(ExchangeRate("20171224", "SEK", "USD", 8.33034829d))
		assert(ExchangeRate.defaultRateLineParser(rawRate) === expectedRate)

		// 2:
		rawRate = "20170327,USD,CRC,564.85"
		expectedRate = Some(ExchangeRate("20170327", "USD", "CRC", 564.85d))
		assert(ExchangeRate.defaultRateLineParser(rawRate) === expectedRate)
	}
}
