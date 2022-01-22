# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from datetime import datetime
from datetime import date
import dateparser

class MySpider(Spider):
    name = 'siab-immo_fr'
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    start_urls = ["https://www.siab-immo.fr/a-louer/1"]
    external_source='Siabimmo_PySpider_france'

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False

        for item in  response.xpath("//ul[@class='listingUL']//li//@onclick").getall():
            item=item.split("href='")[-1].replace("'","")
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            next_page = f"https://www.siab-immo.fr/votre-recherche/{page}"
            if next_page:
                yield Request(
                next_page,
                callback=self.parse,
                meta={"page":page+1})

    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        description=response.xpath("//p[@itemprop='description']/text()").get()
        if description:
            item_loader.add_value("description",description)
        property_type= response.xpath("//p[contains(.,'Type de biens')]/span/text()").get()
        if property_type and "appartement" in property_type.lower():
            item_loader.add_value("property_type","apartment")
        if property_type and ("maison" in property_type.lower() or "villa" in property_type.lower()):
            item_loader.add_value("property_type","house")
        if property_type and "studio" in property_type.lower():
            item_loader.add_value("property_type","studio")
        title=response.xpath("//h1[@itemprop='name']/text()").get()
        if title:
            item_loader.add_value("title",title)
        dontallow=response.xpath("//h1[@itemprop='name']/text()").get()
        if dontallow and "parking" in dontallow.lower():
            return
        external_id=response.xpath("//span[@itemprop='productID']/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id.split(":")[-1])
        
        rent=response.xpath("//p[contains(.,'Prix du bien')]/span/text()").get()
        if rent:
            item_loader.add_value("rent",rent.replace("\n","").split("€")[0].replace(" ",""))
        item_loader.add_value("currency","GBP")
        city=response.xpath("//p[contains(.,'Ville')]/span/text()").get()
        if city:
            item_loader.add_value("city",city.replace("\n","").strip())
        zipcode=response.xpath("//p[contains(.,'Code postal ')]/span/text()").get()
        if zipcode:
            item_loader.add_value("zipcode",zipcode.replace("\n","").strip())
        images=[response.urljoin(x) for x in response.xpath("//figure[@class='mainImg ']//img/@src").getall()]
        if images:
            item_loader.add_value("images",images)
        room_count=response.xpath("//p[contains(.,'Nombre de pièces')]/span/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        square_meters=response.xpath("//p[contains(.,'Surface habitable')]/span/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m")[0])
        item_loader.add_value("landlord_phone","04 91 15 58 14")
        item_loader.add_value("landlord_email","contact@siab-immo.com")
        yield item_loader.load_item()