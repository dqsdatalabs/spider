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
    name = 'visite-andco_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source="VisiteAndco_PySpider_france"
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.visite-andco.fr/location/appartement?prod.prod_type=appt",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.visite-andco.fr/location/maison?prod.prod_type=house",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://www.nnw.com.au/?suburb=&type=Studio&bedrooms=&bathrooms=&min_price=&max_price=&parking=&status=&post_type=nnw_rental&s=",
                ],
                "property_type" : "studio"
            },

        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//a[@class='_gozzbg']/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})



    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link",response.url)
        item_loader.add_value("external_source",self.external_source)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        title=response.xpath("//h1[@class='_1jsj8ch _5k1wy textblock ']/text()").get()
        if title:
            item_loader.add_value("title",title)
        rent=response.xpath("//p[@class='_1oo4n2d _5k1wy textblock ']/text()").get()
        if rent and not "Loué"==rent:
            item_loader.add_value("rent",rent.strip())
        item_loader.add_value("currency","GBP")
        description="".join(response.xpath("//span[@class='_1cf4d7a _5k1wy textblock ']//text()").getall())
        if description:
            item_loader.add_value("description",description)
        adres=response.xpath("//span[.='Localisation']/following-sibling::span/text()").get()
        if adres:
            item_loader.add_value("address",adres)
        city=response.xpath("//span[.='Localisation']/following-sibling::span/text()").get()
        if city:
            item_loader.add_value("city",city.strip().split(" ")[0])
        zipcode=response.xpath("//span[.='Localisation']/following-sibling::span/text()").get()
        if zipcode:
            item_loader.add_value("zipcode",zipcode.strip().split(" ")[-1])

        room_count=response.xpath("//span[contains(.,'Pièces ')]/following-sibling::span/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        bathroom_count=response.xpath("//span[.='Salle d']/following-sibling::span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)
        floor=response.xpath("//span[.='Étage']/following-sibling::span/text()").get()
        if floor:
            item_loader.add_value("floor",floor)
        square_meters=response.xpath("//span[@class='_tr09cu _1pdk5kl  textblock ']/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.strip())
        
        phone=response.xpath("//text()[contains(.,'Téléphone')]").get()
        if phone:
            item_loader.add_value("landlord_phone",phone.split(":")[-1])
        item_loader.add_value("landlord_email"," contact@visite-andco.fr")

        
        




        yield item_loader.load_item()