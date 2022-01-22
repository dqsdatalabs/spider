# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader 
import json
import lxml
import js2xml
import re
 

class MySpider(Spider):
    name = 'puntocasavenezia_it'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'

    external_source = "Puntocasavenezia_Pyspider_italy"
    # LEVEL 1
    start_urls = ['http://puntocasavenezia.it/ads/affitto/immobile']
    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='result-details']//a//@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)


    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        prop_type = ""  
        p_type = "".join(response.xpath("//title//text()").get())
        if p_type and ("Appartamento" in p_type):
            prop_type = "apartment"
        elif p_type and ("Attico" in p_type):
           prop_type = "house"
        elif p_type and ("Cantina" in p_type) :
           prop_type = "studio"
        else:
           return

        item_loader.add_value("property_type", prop_type)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        external_id=response.xpath("//span[@class='property-ref']//text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id.split("Ref."))

        title=response.xpath("//h1[@id='titulo']//text()").get()
        if title:
            item_loader.add_value("title",title)

        item_loader.add_value("city","Venezia")
        item_loader.add_value("address","S. Polo, 2871 - 30125 Venezia (VE)")

        rent=response.xpath("//span[@class='price']//text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[0])
        item_loader.add_value("currency","EUR")

        square_meters=response.xpath("//li[contains(.,'m²')]//text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m²")[0])

        room_count=response.xpath("//li[contains(.,'Loc')]//text()").get() 
        if room_count:
            room_count=room_count.split("Loc")[0]
            item_loader.add_value("room_count",room_count)

        bathroom_count=response.xpath("//li[contains(.,'Bagni') or contains(.,'Bagno')]//text()").get()
        if bathroom_count:
            bathroom_count=bathroom_count.split("Bagn")[0]
            item_loader.add_value("bathroom_count",bathroom_count)

        description=response.xpath("//p[contains(@class,'contitle')]//text()").getall()
        if description:
            item_loader.add_value("description",description)
                  
            images="".join(response.xpath(
                "//script[@type='text/javascript' and contains(.,'multimediaId')]/text()").extract())
            if images:
                img = images.split('"multimediaId":')
                for images in img :
                    if "src" in images:
                        images=images.split('src":"')[-1].split('",')[0]      
                        item_loader.add_value("images", images)
        imagescheck=item_loader.get_output_value("images")
        if not imagescheck:
            images="".join(response.xpath("//script[@type='text/javascript' and contains(.,'multimediaId')]/text()").extract())
            img=images.split('"multimediaId":')
            for images in img:
                if "src" in images:
                    images=images.split('src":"')[-1].split(".jpg")[0]

                    item_loader.add_value("images",images+".jpg")

        floor=response.xpath("//li[contains(.,'Piano')]/text()").get()
        if floor:
            floor=re.findall("\d+",floor)
            item_loader.add_value("floor",floor)


        latitude_longitude = response.xpath(
            "//form[contains(@id,'frmiframe')]//input[@name='defaultLatLng']//@value").get()
        if latitude_longitude:
            latitude = latitude_longitude.split(
                '{ltd:')[1].split(',')[0]
            longitude = latitude_longitude.split(
                ', lng:')[1].split('}')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        item_loader.add_value("landlord_phone", "+39 041 710 408")
        item_loader.add_value("landlord_email", "info@puntocasavenezia.it")
        item_loader.add_value("landlord_name", "Puntocasa")

        yield item_loader.load_item()