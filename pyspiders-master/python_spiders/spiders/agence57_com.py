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
    name = 'agence57_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source="Agence57_PySpider_france"
 
    def start_requests(self):
        start_urls = [
            {"url": "https://www.agence57.com/biens/louer/?price&property_type=appartement&rooms", "property_type": "apartment"},
            {"url": "https://www.agence57.com/biens/louer/?price&property_type=maison&rooms", "property_type": "house"},
        ]  # LEVEL 1
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')
                            })
        
            
    # 1. FOLLOWING
    def parse(self, response): 

        for item in response.xpath("//div[@class='elips_card']/a/@href").getall():
            yield Request(
                response.urljoin(item), 
                callback=self.populate_item, 
                meta={"property_type" : response.meta.get("property_type")},
            )
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source",self.external_source)

        title=response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title)
        rent=response.xpath("//h3[contains(.,'€/mois')]/text()").get()
        if rent:
            rent=rent.split("-")[-1].strip().split("€")[0].strip()
            item_loader.add_value("rent",rent)
        city=response.xpath("//h3[contains(.,'€/mois')]/text()").get()
        if city:
            item_loader.add_value("city",city.split("-")[0])
        square_meters= "".join(response.xpath("//div[@class='row mt-3 mb-3 p-4 text-center shadow-sm rounded']//div[contains(.,'m²')]/text()").getall())
        if square_meters:
            square_meters=re.findall("\d+",square_meters)
            item_loader.add_value("square_meters",square_meters)
        description=" ".join(response.xpath("//div[@class='col-lg-12']//p//text()").getall())
        if description:
            item_loader.add_value("description",description)
        deposit=response.xpath("//p[contains(.,'Dépôt de garantie')]/text()").getall()
        if deposit:
            for i in deposit:
                if "Dépôt de garantie" in i:
                    item_loader.add_value("deposit",i.split(":")[-1].split("€")[0])
            
        images=response.xpath("//figure//a//img//@src").getall()
        if images:
            item_loader.add_value("images",images)
        room_count=response.xpath("//i[@class='fas fa-bed fa-2x']/parent::div/following-sibling::div/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.strip())
        roomcheck=item_loader.get_output_value("room_count")
        if not roomcheck:
            room=response.xpath("//h2[contains(.,'pièces')]/text()").get()
            if room:
                room=room.split("pièces")[0]
                room=re.findall("\d+",room)
                item_loader.add_value("room_count",room)
        bathroom_count=response.xpath("//i[@class='fas fa-bath fa-2x']/parent::div/following-sibling::div/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.strip())
        item_loader.add_value("landlord_name","Agence 57")
        yield item_loader.load_item()
