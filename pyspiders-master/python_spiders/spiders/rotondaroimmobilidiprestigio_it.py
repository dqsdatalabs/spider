# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re

class MySpider(Spider):
    name = 'rotondaroimmobilidiprestigio_it' 
    execution_type='testing' 
    country='italy'
    locale='it' 
    external_source = "Rotondaroimmobilidiprestigio_PySpider_italy"
    custom_settings = {
        "PROXY_US_ON": True
    }
 
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.rotondaroimmobilidiprestigio.it/ricerca.php?tipologia=4&mqmin=&comune=&contratto=Affitto&mqmax=&zona=&zonal=&locali1=&locali2=&price1=&price2=&Riferimento=",
                ],      
                "property_type" : "apartment"
            },
            {
                "url" : [
                    "https://www.rotondaroimmobilidiprestigio.it/ricerca.php?tipologia=7&mqmin=&comune=&contratto=Affitto&mqmax=&zona=&zonal=&locali1=&locali2=&price1=&price2=&Riferimento=",
                    "https://www.rotondaroimmobilidiprestigio.it/ricerca.php?tipologia=12&mqmin=&comune=&contratto=Affitto&mqmax=&zona=&zonal=&locali1=&locali2=&price1=&price2=&Riferimento=",
                    "https://www.rotondaroimmobilidiprestigio.it/ricerca.php?tipologia=13&mqmin=&comune=&contratto=Affitto&mqmax=&zona=&zonal=&locali1=&locali2=&price1=&price2=&Riferimento=",
                    "https://www.rotondaroimmobilidiprestigio.it/ricerca.php?tipologia=34&mqmin=&comune=&contratto=Affitto&mqmax=&zona=&zonal=&locali1=&locali2=&price1=&price2=&Riferimento="
                ],      
                "property_type" : "house"
            }
          
        ] 
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for url in response.xpath("//li[@class='cp-list']//div[@class='product-title']/a/@href").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        pagination = response.xpath("//li[a[@class='sel']]/following-sibling::li[1]/a/@href").get()
        if pagination:
            follow_url = "https://www.rotondaroimmobilidiprestigio.it/ricerca.php"+pagination
            yield Request(follow_url, callback=self.parse, meta={"property_type":response.meta["property_type"]})
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_xpath("title", "//h1/text()")

        address = response.xpath("//div[@class='container']/a/following-sibling::strong[1]//text()").get()
        if address:
            item_loader.add_value("address", address)
            # if "," in address:
        item_loader.add_value("city", "Milano")

        external_id = response.xpath("//p//strong[.='Riferimento']/following-sibling::text()[1]").get()
        if external_id:
            item_loader.add_value("external_id", external_id.replace(":","").strip())

        rent_string = response.xpath("//p//strong[.='Canone']/following-sibling::text()[1]").get()
        if rent_string:
            item_loader.add_value("rent_string", rent_string.replace(":","").strip())

        room_count = response.xpath("//p//strong[.='Locali']/following-sibling::text()[1]").get()
        if room_count:
            item_loader.add_value("room_count", room_count.replace(":","").strip())

        square_meters = response.xpath("//p//strong[.='Superficie']/following-sibling::text()[1]").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.replace(":","").strip())

        energy_label = response.xpath("//p//strong[.='Classificazione Energetica']/following-sibling::text()[1]").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.replace(":","").split("-")[0].strip())
     
        utilities = response.xpath("//p//strong[.='Spese Condominiali']/following-sibling::text()[1]").get()
        if "all'anno" in utilities.lower():
            utilities=utilities.split("€")[1].split("all'anno")[0]
            utilities=utilities.replace(".","")
            utilities=int(float(utilities)/12)
            item_loader.add_value("utilities", utilities)

        description = " ".join(response.xpath("//div[@class='col-md-8 col-xs-12']/p//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())

        images = [x for x in response.xpath("//div[@class='item  big']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        floor=response.xpath("//strong[.='Piano']/following-sibling::text()").get()
        if floor:
            item_loader.add_value("floor",floor.split(":")[-1])

        latitude_longitude = response.xpath("//script[@type='application/ld+json']//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('"latitude": "')[1].split('",')[0]
            longitude = latitude_longitude.split('"longitude": "')[1].split('"')[0]    
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

 
        item_loader.add_value("landlord_phone", "+390248516282")
        item_loader.add_value("landlord_email", "info@rotondaroimmobili.com")
        item_loader.add_value("landlord_name", "ROTONDARO AGENZIA IMMOBILIARE")
     

        yield item_loader.load_item()