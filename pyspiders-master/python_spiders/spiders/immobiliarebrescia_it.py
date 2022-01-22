# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.http import request
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
 
class MySpider(Spider):
    name = 'immobiliarebrescia_it'
    execution_type = 'testing'
    country = 'italy' 
    locale = 'it'
    external_source = "Immobiliarebrescia_PySpider_italy"
    start_urls = ['https://immobiliarebrescia.it/immobili-in-affitto/']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        

        for item in response.xpath("//div[@class='mh-grid__1of3']"):
            follow_link = item.xpath(".//div/article/a/@href").get()
            yield Request(follow_link, callback=self.populate_item)




    def populate_item(self,response):

        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)


        property_type = response.xpath("//strong[contains(text(),'Tipologia immobile')]/following-sibling::a/text()").get()
        if "commercecial" in property_type:
            return
        else:
            item_loader.add_value("property_type","apartment")

        city = response.xpath("//strong[contains(text(),'Città')]/following-sibling::a/text()").get()
        if city:
            item_loader.add_value("city",city.strip())

        area = response.xpath("//strong[contains(text(),'Zona')]/following-sibling::a/text()").get()
        street = response.xpath("//strong[contains(text(),'Via')]/following-sibling::a/text()").get()
        if street:
            address = street.strip() + " - " + area.strip() + " - " + city.strip()
            if address:
                item_loader.add_value("address",address)
        else:
            address = area.strip() + " - " + city.strip()
            if address:
                item_loader.add_value("address",address)

        squared_meters = response.xpath("//strong[contains(text(),'Superficie')]/following-sibling::text()").get()
        if squared_meters:
            squared_meters = squared_meters.split()[0]
            item_loader.add_value("square_meters",squared_meters.strip())

        bathroom_count = response.xpath("//strong[contains(text(),'Bagni')]/following-sibling::text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)

        room_count = response.xpath("//strong[contains(text(),'Stanze da letto')]/following-sibling::text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        else:
            return

        desc = " ".join(response.xpath("//h2/span/span/span[last()]/text()").getall())
        if desc:
            item_loader.add_value("description",desc)
        else:
            desc = " ".join(response.xpath("//div[@class='mh-estate__section mh-estate__section--description']/p//text()").getall())
            if desc:
                item_loader.add_value("description",desc)

        external_id = response.xpath("//span[text()='ID:']/following-sibling::text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id.strip())

        title = response.xpath("//h1[@class='mh-top-title__heading']/text()").get()
        if title:
            item_loader.add_value("title",title)

        rent = response.xpath("//div[@class='mh-estate__details__price__single']/text()").get()
        if rent:
            rent = rent.split()[0].strip("€").replace(".","")
            item_loader.add_value("rent",rent)

        floor_image = response.xpath("//a[@class='mh-estate__plan-thumbnail-wrapper mh-popup']/@href").get()
        if floor_image:
            item_loader.add_value("floor_plan_images",floor_image)

        images = response.xpath("//div[@class='swiper-slide']/a/@href").getall()
        if images:
            item_loader.add_value("images",images)
            item_loader.add_value("external_images_count",len(images))

        item_loader.add_value("currency","EUR")
        item_loader.add_value("landlord_phone","+39 06 5756087")
        item_loader.add_value("landlord_email","immobiliarebrescia@tiscali.it ")
        item_loader.add_value("landlord_name","BRESCIA REAL ESTATE")
        item_loader.add_value("external_source",self.external_source)


        yield item_loader.load_item()

