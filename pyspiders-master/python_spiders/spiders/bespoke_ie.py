from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from urllib.parse import urljoin
import re




class MySpider(Spider):

    name = 'baspoke_ie'
    execution_type='testing'
    country='ireland'
    locale='en'
    external_source = "Baspokeie_PySpider_ireland"
    custom_settings = {
    "HTTPCACHE_ENABLED": False,
    "CONCURRENT_REQUESTS" : 2,
    "AUTOTHROTTLE_ENABLED": True,
    "AUTOTHROTTLE_START_DELAY": .5,
    "AUTOTHROTTLE_MAX_DELAY": 2,
    "RETRY_TIMES": 3,
    "DOWNLOAD_DELAY": 2,
    "PROXY_TR_ON" : True,
    "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:94.0) Gecko/20100101 Firefox/94.0"
    }

    start_urls = ["https://www.bespoke.ie/properties-for-rent/page/1/"]
    
    # 1. FOLLOWING
    def parse(self, response):

        for url in response.xpath("//article[@class='property-item clearfix']/h4/a/@href").getall():

            yield Request(
                url, 
                callback=self.populate_item, 
            )


        next_url = response.xpath("//a[@class='real-btn real-btn-jump rh_arrows_right'][text()='Next']/@href").get()
        print(next_url)
        if  next_url:
            
            yield Request(
                next_url, 
                callback=self.parse,
            )

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source",self.external_source)
        title = response.xpath("//h1[@class='page-title']/span/text()").get()
        if title:
            item_loader.add_value("title",title)
            address = title.split(",",1)[-1]
            item_loader.add_value("address",address)

        external_id = response.xpath("//span[@class='property-meta-id']/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id.strip())

        rent = response.xpath("//span[@class='price-and-type']/text()[contains(.,'€')]").get()
        if rent:
            rent = rent.replace("€","").replace(",","").strip()
            item_loader.add_value("rent",rent)

        prop_type = response.xpath("//span[@class='price-and-type']/small/text()").get()
        if prop_type:
            prop_type = prop_type.replace("-","").strip()
            if "flat"  in prop_type :
                prop_type = "apartment"
            item_loader.add_value("property_type",prop_type)

        desc = response.xpath("//div[@class='content clearfix']/p/text()").get()
        if desc:
            item_loader.add_value("description",desc)

        position = " ".join(response.xpath("//script[contains(text(),'\"lat\"')]/text()").getall())
        if position:
            lat = re.search('"lat":"([\d.]+)',position).group(1)
            long = re.search('"lng":"([\d.-]+)',position).group(1)
            item_loader.add_value("latitude",lat)
            item_loader.add_value("longitude",long)

        landlord_name = response.xpath("//div[@class='agent-detail clearfix']/div/h3/text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name",landlord_name)

        landlord_phone = response.xpath("//li[@class='office']/a/text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone",landlord_phone)

        item_loader.add_value("landlord_email","info@bespoke.ie")

        item_loader.add_value("currency","EUR")

        room = response.xpath("//span[@class='property-meta-bedrooms']/text()").get()
        if room:
            room = room.split("\xa0")[0].strip()
            item_loader.add_value("room_count",room)

        bathroom = response.xpath("//span[@class='property-meta-bath']/text()").get()
        if bathroom:
            bathroom = room.split("\xa0")[0].strip()
            item_loader.add_value("bathroom_count",bathroom)

        item_loader.add_value("city","Dublin")

        images = response.xpath("//li/a/img/@src").getall()
        if images:
            item_loader.add_value("external_images_count",len(images))
            item_loader.add_value("images",images)
        

        yield item_loader.load_item()