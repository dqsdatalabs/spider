# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from  geopy.geocoders import Nominatim
import re

class MySpider(Spider):
    name = 'immobiliere_du_palais_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Immobiliere_du_palais_PySpider_france'
    def start_requests(self):
        start_urls = [
            {"url": "https://www.immobiliere-du-palais.fr/bien-recherche/?type=appartement&status=location", "property_type": "apartment"},
            {"url": "https://www.immobiliere-du-palais.fr/bien-recherche/?type=maison&status=location", "property_type": "house"},
        ]  # LEVEL 1

        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='rh_prop_card__details']/h3/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})

# 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
  
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        
        title = response.xpath("//h1[@class='rh_page__title']/text()").get()
        if title:
            item_loader.add_value("title", title)
          
        external_id = response.xpath("//p[@class='id']/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.replace("\u00a0",""))

        rent = "".join(response.xpath("//p[@class='price']/text()").getall())
        if rent:
            rent = rent.replace("\u00a0","").replace("€","").strip()
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")
        
        square_meters=response.xpath("((//h4[contains(.,'Surface')])[1]/following-sibling::div/span/text())[1]").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split(".")[0].strip())
        
        room_count=response.xpath("(//h4[contains(.,'Chambre')])[1]/following-sibling::div/span/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)

        bathroom_count=response.xpath("(//h4[contains(.,'bain')])[1]/following-sibling::div/span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)   
        
        address="".join(response.xpath("//p[@class='rh_page__property_address']/text()").getall())
        if address:
            item_loader.add_value("address", address.strip())
            
            city = address.split(" ")[1]
            if city:
                item_loader.add_value("city", city)
            zipcode = address.split(" ")[0].strip()
            if zipcode.isdigit():
                item_loader.add_value("zipcode", str(zipcode))

        desc="".join(response.xpath("//div[@class='rh_content']/p/text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip().replace("\n",""))
            
        images=[x for x in response.xpath("//a[@rel='gallery_real_homes']/@href").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        item_loader.add_value("landlord_name", "IMMOBILIERE DU PALAIS")
        item_loader.add_value("landlord_phone", "04 42 26 37 70")
        item_loader.add_value("landlord_phone", "info@immobiliere-du-palais.fr")
        
        yield item_loader.load_item()