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
import dateparser

class MySpider(Spider):
    name = 'lamaisondeluxe_com' 
    execution_type='testing'
    country='france'
    locale='fr'
    external_source="Lamaisondeluxe_com_PySpider_france"
    custom_settings = {"HTTPCACHE_ENABLED": False}



    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.lamaisondeluxe.com/_actions.php?token=&marche=1&transaction=2&type=1&action=show_annonces_continuous_scroll&offset=1",
                ],
                "property_type":"apartment"
            },
            {
                "url" : [
                    "https://www.lamaisondeluxe.com/_actions.php?token=&marche=1&transaction=2&type=8&action=show_annonces_continuous_scroll&offset=1",
                ],
                "property_type":"house"
            }            
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[@class='lo-annonce-header clearfix']//a/@href").getall():
            follow_url = item
            yield Request(follow_url, callback=self.populate_item,meta={"property_type" : response.meta.get("property_type")},)
    
        test_end = response.xpath("//div[@class='lo-annonce-header clearfix']//a/@href").get().split("offset=")[-1]
        response_end = response.url.split("offset=")[-1]

        prop = response.meta.get("property_type")
        if (test_end == response_end) and prop=="apartment":
            num = int(test_end) + 1
            new_url = f"https://www.lamaisondeluxe.com/_actions.php?token=&marche=1&transaction=2&type=1&action=show_annonces_continuous_scroll&offset={num}"
            yield Request(new_url, callback=self.parse,meta={"property_type" : response.meta.get("property_type")})

        if (test_end == response_end) and prop=="house":
            num = int(test_end) + 1
            new_url = f"https://www.lamaisondeluxe.com/_actions.php?token=&marche=1&transaction=2&type=8&action=show_annonces_continuous_scroll&offset={num}"
            yield Request(new_url, callback=self.parse,meta={"property_type" : response.meta.get("property_type")})




    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        prop = response.meta.get("property_type")
        item_loader.add_value("property_type",prop)
        item_loader.add_value("external_source",self.external_source)
        item_loader.add_value("external_link",response.url)

        title1 = response.xpath("//h1/span[@class='line-1']/text()").get()
        title2 = response.xpath("//h1/span[@class='line-2']/text()").get()
        if title1:
            item_loader.add_value("title",title1 +"/"+ title2)
        if title2:
            item_loader.add_value("address",title2)

            zipcode = title2.split("-")[0]
            city = title2.split("-")[-1]
            if zipcode:
                item_loader.add_value("zipcode",zipcode)

            if city:
                item_loader.add_value("city",city)
        
        external_id = response.xpath("//p[contains(text(),'Réf.')]").get()
        if external_id:
            external_id = external_id.split(":")[-1].strip()
            item_loader.add_value("external_id",external_id)

        
        rent = response.xpath("//span[@class='price']/span/text()").get()
        if rent:
            rent = rent.split("€")[0].replace(" ","")
            item_loader.add_value("rent",rent)

        item_loader.add_value("currency","EUR")

        room = response.xpath("//span[contains(text(),'Pièce')]/text()").get()
        if room:
            room = room.split()[0]
            item_loader.add_value("room_count",room)

        bath = response.xpath("//li[@class='nombre_sdb']/span[@class='title']/text()").get()
        if bath:
            item_loader.add_value("bathroom_count",bath.strip())

        square = response.xpath("//li[@class='surface_habitable']/span[@class='title']/text()").get()
        if square:
            item_loader.add_value("square_meters",square.replace(" ","").strip())


        desc = " ".join(response.xpath("//div[@class='lo-box-content clearfix']/p/text()").getall())
        if desc:
            item_loader.add_value("description",desc)

        floor = response.xpath("//span[contains(text(),'Etage')]/text()").get()
        if floor:
            floor = floor.split(":")[-1].strip()
            item_loader.add_value("floor",floor)

        images = ["https://www.lamaisondeluxe.com" + img for img in response.xpath("//img[@class='landscape lazy-slick']/@data-original").getall()]
        if images:
            item_loader.add_value("images",images)

        item_loader.add_value("landlord_name","LaMaisonDeLuxe")
        item_loader.add_value("landlord_phone","0678367687")

        yield item_loader.load_item()