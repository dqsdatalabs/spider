# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
import re


class MySpider(Spider):
    name = 'minasidor_haparandabostader_se'
    execution_type='testing'
    country='swenden'
    locale='sv'
    external_source = "MinasidorHaparandabostader_Pyspider_sweden"
  

    def start_requests(self):
        url="https://minasidor.haparandabostader.se/soek-ledigt/lediga-bostaeder/"
        yield Request(url=url,callback=self.parse,)
    def parse(self, response): 
        for item in response.xpath("//div[@class='col-md-3 column']/a/@href").getall():
            f_url = response.urljoin(item) 
            yield Request(f_url,callback=self.populate_item,)
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", self.external_source) 
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type","apartment")
        title=response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title)

        external_id="".join(response.url)
        if external_id:
            external_id = external_id.split("bostad-")[1].split("/")[0]
            item_loader.add_value("external_id",external_id)
        adres=response.xpath("//h2[@class='text-uppercase']/text()").get()
        if adres:
            item_loader.add_value("address",adres)
            
        zipcode=response.xpath("//dt[contains(.,'Postadress')]//following-sibling::dd[2]//text()").get()
        if zipcode:
            item_loader.add_value("zipcode",zipcode)

        city=response.xpath("//dt[contains(.,'Postadress')]//following-sibling::dd[3]//text()").get()
        if city:
            item_loader.add_value("city",city)
            
        room=response.xpath("//h2[contains(.,'rum')]/text()").get()
        if room:
            item_loader.add_value("room_count",room.split("rum")[0])
        images=[response.urljoin(x) for x in response.xpath("//div[@class='thumbnails']/a/img/@src").getall()]
        if images:
            item_loader.add_value("images",images)
        description="".join(response.xpath("//p[@class='text-justify']/text()").getall())
        if description:
            description = re.sub('\s{2,}', ' ', description.strip().replace("\n",""))
            item_loader.add_value("description",description)
        square_meters=response.xpath("//dt[.='Yta']/following-sibling::dd/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split(",")[0])
        rent=response.xpath("//dt[.='Hyra']/following-sibling::dd/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("Kr")[0].replace("\xa0",""))
        item_loader.add_value("currency","SEK")

        item_loader.add_value("landlord_name","Haparanda bostäder")
        item_loader.add_value("landlord_phone","0922-260 31")
        item_loader.add_value("landlord_email","kundtjanst.bolagen@haparanda.se")
            

        yield item_loader.load_item()