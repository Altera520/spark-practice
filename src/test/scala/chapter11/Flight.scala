package chapter11

case class Flight(DEST_COUNTRY_NAME: String,
                  ORIGIN_COUNTRY_NAME: String,
                  count: BigInt)

case class FlightMetadata(count: BigInt,
                          randomData: BigInt)
