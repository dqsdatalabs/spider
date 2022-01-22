# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import math
from  geopy.geocoders import Nominatim
import re

class MySpider(Spider):
    name = 'limmobilieredeprovence_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source = "Limmobilieredeprovence_com_PySpider_france_fr"
    
    def start_requests(self):
        start_urls = [
            {"url": "http://www.limmobilieredeprovence.com/fr/locations-biens-immobiliers.htm?_typebase=2&_typebien%5B%5D=1", "property_type": "apartment"}
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='texte']//div[contains(@class,'one-page-inner')]//a[contains(.,'Détail')]/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
            
# 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", self.external_source)

        title = response.xpath("//h1//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        
        rent="".join(response.xpath("//div[@id='prix-immobilier-detail']//text()").getall())
        if rent:
            price=rent.replace(" ","")
            item_loader.add_value("rent_string", price)
        
        square_meters=response.xpath("//div[@class='champsSPEC']/div[contains(.,'habitable')]/div[2]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m²")[0].strip())
        
        room_count=response.xpath("//div[@class='champsSPEC']/div[contains(.,'chambre')]/div[2]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
            
        bathroom_count=response.xpath("//div[@class='champsSPEC']/div[contains(.,'Nombre de salles')]/div[2]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
            
        address="".join(response.xpath("//div[@id='lieu-detail']/text()").getall())
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.strip().split(" ")[0])
            item_loader.add_value("zipcode", address.split("-")[1].strip())
            
        external_id=response.xpath("//div[@id='reference-detail']/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(".")[1].strip())

        desc="".join(response.xpath("//div[@id='texte-detail']//text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())
        
        utilities = response.xpath("//div[@id='texte-detail']//text()[contains(.,'PRIX') and contains(.,'charge')]").get()
        if utilities:
            utilities = utilities.split("dont")[1].strip().split(" ")[0]
            item_loader.add_value("utilities", utilities)
        
        images=[x for x in response.xpath("//div/div/a/img/@src[not(contains(.,'_images')) and not(contains(.,'logoFOOTER'))]").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        item_loader.add_value("landlord_name","LIMMOBILIERE DE PROVENCE")
        item_loader.add_value("landlord_phone"," 33 04 42 18 58 40")
        item_loader.add_value("landlord_email","limmobilieredeprovence@orange.fr")
        
        floor=response.xpath("//div[@class='champsSPEC']/div[contains(.,'Etage')]/div[2]/text()").get()
        if floor:
            item_loader.add_value("floor", floor.strip())
    
        yield item_loader.load_item()