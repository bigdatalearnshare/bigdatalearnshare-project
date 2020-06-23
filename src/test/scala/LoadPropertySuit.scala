import org.scalatest.FlatSpec
import tech.bigdatalearnshare.recommendation.utils.ConfProperties

/**
  * @Author fjs
  * @Date 2020-05-11
  */
class LoadPropertySuit extends FlatSpec {

  "load configure properties" should "work" in {
    println(ConfProperties.ZK_SERVERS)
    println(ConfProperties.BROKERS)
    assert(!ConfProperties.USERNAME.equals("<unknown>"))
  }

}
