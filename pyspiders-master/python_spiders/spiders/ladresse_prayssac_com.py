# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'ladresse_prayssac_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source = "Ladresse_Prayssac_PySpider_france"
    start_urls = ['https://www.ladresse-prayssac.com/annonces/transaction/Location.html']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[@class='products-cell']/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            prop_type = item.xpath(".//div[@class='products-name']/text()").get()
            if get_p_type_string(prop_type):
                yield Request(follow_url, callback=self.populate_item, meta={"property_type": get_p_type_string(prop_type)})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)


        title=response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title.replace("\u00e8","").replace("\u00b2",""))

        external_id=response.xpath("//span[contains(@itemprop,'name')]//text()[contains(.,'Ref.')]").get()
        if external_id:
            item_loader.add_value("external_id",external_id.split("Ref. :"))   

        description=response.xpath("//div[contains(@class,'content-desc')]//text()").getall()
        if description:
            item_loader.add_value("description",description)

        room_count=response.xpath("//li[contains(.,'Pièces')]//text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.split("Pièces"))
        else:
            room_count=response.xpath("//li//span[contains(.,'chambres')]//text()").get()
            if room_count:
                item_loader.add_value("room_count",room_count.split("chambres"))

        square_meters="".join(response.xpath("//li//span[contains(.,'m²')][1]//text()").get())
        if square_meters:
            square_meters = square_meters.split("m²")[0].strip()
            item_loader.add_value("square_meters",square_meters)

        rent=response.xpath("//div[@class='product-price']//span[contains(@class,'alur_loyer_price')]//text()").get()
        if  rent:
            rent = rent.split("Loyer")[1].split("€")[0].strip()
            item_loader.add_value("rent",rent)
        item_loader.add_value("currency","EUR")

        images=[response.urljoin(x) for x in response.xpath("//ul[@class='slides']//li//img//@src").getall()]
        if images:
            item_loader.add_value("images",images)

        item_loader.add_value("landlord_phone", "05.65.36.54.19")
        item_loader.add_value("landlord_name", "L'aDRESSE - Prayssac")

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("appartement" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("maison" in p_type_string.lower() or "duplex" in p_type_string.lower() or "villa" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None