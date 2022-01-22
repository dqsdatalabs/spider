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
    name = 'kolpavanderhoek_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    external_source = "Kolpavanderhoek_PySpider_netherlands_nl"
 
        
    def start_requests(self):
        start_urls = [
            {
                "url": "https://www.kolpavanderhoek.nl/overzicht/wonen/?_object_koophuur=huur&_object_status=beschikbaar%2Cverhuurd_onder_voorbehoud&_object_type=appartement", 
                "property_type": "apartment"
            },
            {
                "url": "https://www.kolpavanderhoek.nl/overzicht/wonen/?_object_koophuur=huur&_object_status=beschikbaar%2Cverhuurd_onder_voorbehoud&_object_type=woonhuis", 
                "property_type": "house"
            }
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),"base_url":url.get("url")})
    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        base_url = response.meta.get("base_url")
        seen = False
        for url in response.xpath("//a[@class='object-item']/@href").getall():
            seen = True
            yield Request(url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})


        if page == 2 or seen:
            follow_url = f"{base_url}&_paged={page}"
            yield Request(follow_url, callback=self.parse, meta={"property_type":response.meta["property_type"], "page": page + 1,"base_url":base_url})
       
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", self.external_source)
        
        prop_type = response.meta.get("property_type")
        item_loader.add_value("property_type", prop_type)
        
        title = response.xpath("//h1/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        address = response.xpath("//h1/text()").get()
        if address:
            address = re.sub('\s{2,}', ' ', address.strip())
            item_loader.add_value("address", address)

        item_loader.add_value("external_link", response.url)

        city=response.xpath("//h1//text()").get()
        if city:
            item_loader.add_value("city",city.split(",")[-1].strip())

        zipcode=response.xpath("//tr[./td='Postcode']/td[2]/text()").get()
        if zipcode:
            item_loader.add_value("zipcode",zipcode.strip())
        desc = "".join(response.xpath("//div[@class='content content--truncated' and ./h2[.='Omschrijving']]//p//text()").extract())
        desc = re.sub('\s{2,}', ' ', desc)
        if desc:
            item_loader.add_value("description", desc.strip())
            
        if "gemeubileerd" in desc.lower() or "furnished" in desc.lower():
            item_loader.add_value("furnished", True)
    
        square_meters = response.xpath("//tr[./td='Oppervlakte']/td[2]/text()").get()
        if square_meters:
            square_meters = square_meters.split(" ")[0]
        item_loader.add_value("square_meters", square_meters)
  
        room_count = response.xpath("//tr[./td='Aantal slaapkamers']/td[2]/text()").get()
        if room_count:
            room_count = room_count.split(" ")[0]
            item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("//tr[./td='Aantal kamers']/td[2]/text()").get()
            if room_count:
                room_count = room_count.split(" ")[0]
                item_loader.add_value("room_count", room_count)
        bathroom_count = response.xpath("//tr[./td='Aantal badkamers']/td[2]/text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.split(" ")[0]
            item_loader.add_value("bathroom_count", bathroom_count)

        utility  = response.xpath("//tr/td[.='Servicekosten']/following-sibling::td/text()").get()
        if utility :
            utilities =  utility.split(" ")[0].strip()
            item_loader.add_value("utilities", utilities)

        images = [x for x in response.xpath("//div[@class='media-grid media-grid--fotos']/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
        

        floor_plan_images = [x for x in response.xpath("//div[@class='media-grid media-grid--plattegrond']/a/@href").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        price = response.xpath("//tr[./td='Prijs']/td[2]/text()").get()
        if price:
            item_loader.add_value("rent_string", price)

        item_loader.add_value("landlord_name", "Kolpa van der Hoek")
        item_loader.add_value("landlord_phone", "010 – 422 6144")
        item_loader.add_value("landlord_email", "rotterdam@kolpavanderhoek.nl")

        yield item_loader.load_item()