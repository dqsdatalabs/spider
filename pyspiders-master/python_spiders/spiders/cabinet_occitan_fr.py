# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector 
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from urllib.parse import urljoin


class MySpider(Spider):
    name = 'cabinet_occitan_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    
    def start_requests(self):

        start_urls = [
            {
                "url" : "http://cabinet-occitan.fr/recherche/Location/maison/toutes_villes/?prix_mini=Min&prix_maxi=Max#.X6OLJ2gzbIU",
                "property_type" : "house"
            },
            {
                "url" : "http://cabinet-occitan.fr/recherche/Location/appartement/toutes_villes/?prix_mini=Min&prix_maxi=Max#.X6OK0WgzbIV",
                "property_type" : "apartment"
            },
        ] #LEVEL-1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[contains(@class,'titre_bandeau')]/a/@href").extract():
            f_url = response.urljoin(item)
            yield Request(
                f_url, 
                callback=self.populate_item, 
                meta={"property_type" : response.meta.get("property_type")},
            )
        
        next_page = response.xpath("//a[.='>']/@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse,
                meta={"property_type" : response.meta.get("property_type")},
            )
           
        
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Cabinetoccitan_PySpider_"+ self.country + "_" + self.locale)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        
        title=response.xpath("//div[contains(@class,'titre')]/text()").get()
        if title:
            item_loader.add_value("title", title)
            item_loader.add_value("address", title.split("-")[-1].strip())
        
        rent="".join(response.xpath("//div[contains(@class,'prix')]/text()").getall())
        if rent:
            price=rent.replace(" ","")
            item_loader.add_value("rent_string", price)
        
        room_count=response.xpath("//dl[contains(@class,'tiret_bottom')]/div/span[contains(.,'Pièce')]//following-sibling::span/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        
        bathroom = response.xpath("//dl[contains(@class,'tiret_bottom')]/div/span[contains(.,'Salle')]//following-sibling::span/text()").get()
        if bathroom:
            item_loader.add_value("bathroom_count", bathroom)
        
        square_meters=response.xpath("//dl[contains(@class,'tiret_bottom')]/div/span[contains(.,'Surface')]//following-sibling::span/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split(".")[0])
        
        elevator=response.xpath(
            "//dl[contains(@class,'tiret_bottom')]/div/span[contains(.,'Ascenseur')]//following-sibling::span/text()[contains(.,'OUI')]"
            ).get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        desc="".join(response.xpath("//div/p[contains(@style,'text')]/text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())

        if "garantie" in desc:
            deposit=desc.split("garantie")[1].split("euros")[0].replace(':','').strip()
            item_loader.add_value("deposit", deposit)
        
        external_id=response.xpath("//p[@class='reference']/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[1].strip())
            
        images=[response.urljoin(x) for x in response.xpath("//img[@class='img-polaroid']/@src").getall()]
        if images:
            item_loader.add_value("images", images)

        item_loader.add_value("landlord_name", 'Cabinet Occitan')
        item_loader.add_value("landlord_phone", '05 63 66 05 55')
        
        yield item_loader.load_item()

