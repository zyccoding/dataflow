import java.io.File
import java.net.InetAddress

import com.maxmind.db.CHMCache
import com.maxmind.geoip2.DatabaseReader
import com.maxmind.geoip2.model.CityResponse

/**
  * Created by rzx on 2016/9/13.
  */
object GeoipTest {
  def main(args: Array[String]): Unit = {
    val url2 = "D:/test/GeoLite2-City.mmdb"
    val geoDB = new File(url2)
    val geoIPResolver = new DatabaseReader.Builder(geoDB).withCache(new CHMCache()).build()
    val intAddress = InetAddress.getByName("118.2.3.204")
    val geoResponse = geoIPResolver.city(intAddress)
    val result = resolve_ip(geoResponse)
    println(result)

  }

  def resolve_ip(resp: CityResponse): (String) = {
    //                 (resp.getCountry.getNames.get("zh-CN").concat("|").concat(resp.getSubdivisions.get(0).getNames.get("zh-CN")).concat("|").concat(resp.getCity.getNames.get("zh-CN")))
    (resp.getCountry.getNames.get("zh-CN"), resp.getSubdivisions.get(0).getNames.get("zh-CN"), resp.getCity.getNames.get("zh-CN"))._1.concat("|") + (resp.getCountry.getNames.get("zh-CN"), resp.getSubdivisions.get(0).getNames.get("zh-CN"), resp.getCity.getNames.get("zh-CN"))._2.concat("|") +
      (resp.getCountry.getNames.get("zh-CN"), resp.getSubdivisions.get(0).getNames.get("zh-CN"), resp.getCity.getNames.get("zh-CN"))._3
  }
}
