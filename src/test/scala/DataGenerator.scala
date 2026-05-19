import java.io.{BufferedWriter, FileWriter}
import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, ZoneOffset}
import java.util.UUID
import scala.util.Random

// Run with: sbt "runMain analytics.DataGenerator"
object DataGenerator extends App {

  val rng = new Random(42) // fixed seed = reproducible output
  val formatter = DateTimeFormatter.ISO_INSTANT
  val outputPath = "data/events-sample.jsonl"

  // 30 accounts, events spread across last 7 days
  val accountIds = (1 to 30).map(i => f"account-$i%03d")
  val startEpoch = LocalDateTime.now().toEpochSecond(ZoneOffset.UTC)
  val windowSecs = 7L * 24 * 60 * 60 // 7 days in seconds

  def randomTs(): String = {
    val offsetSecs = (rng.nextDouble() * windowSecs).toLong
    Instant.ofEpochSecond(startEpoch - offsetSecs).toString
  }

  def eventId(): String = UUID.randomUUID().toString

  def amount(): BigDecimal =
    BigDecimal(rng.nextInt(490) + 10).setScale(2) // 10.00 – 500.00

  val writer = new BufferedWriter(new FileWriter(outputPath))

  var totalEvents = 0

  accountIds.foreach { accountId =>
    val initialBalance = BigDecimal(rng.nextInt(900) + 100).setScale(2) // 100–1000

    // every account gets a creation event
    val createdTs = randomTs()
    writer.write(
      s"""{"eventType":"AccountCreated","eventId":"${eventId()}","occurredAt":"$createdTs","accountId":"$accountId","initialBalance":$initialBalance}"""
    )
    writer.newLine()
    totalEvents += 1

    // 80% of accounts get some activity; 20% created but never used
    if (rng.nextDouble() > 0.2) {
      val numEvents = rng.nextInt(28) + 3 // 3–30 events per active account

      (1 to numEvents).foreach { _ =>
        val isDebit = rng.nextDouble() > 0.4 // 60% debits, 40% credits
        val eventType = if (isDebit) "AccountDebited" else "AccountCredited"
        writer.write(
          s"""{"eventType":"$eventType","eventId":"${eventId()}","occurredAt":"${randomTs()}","accountId":"$accountId","amount":${amount()}}"""
        )
        writer.newLine()
        totalEvents += 1
      }
    }
  }

  // inject 15 malformed events so DLQ analysis has something to work with
  (1 to 15).foreach { _ =>
    writer.write(
      s"""{"eventType":"UnknownEvent","garbage":true,"accountId":"account-bad-${rng.nextInt(5)}"}"""
    )
    writer.newLine()
    totalEvents += 1
  }

  writer.close()
  println(s"Generated $totalEvents events → $outputPath")
}