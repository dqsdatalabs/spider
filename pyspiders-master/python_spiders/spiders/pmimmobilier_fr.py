# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json, re


class MySpider(Spider):
    name = 'pmimmobilier_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Pmimmobilier_PySpider_france_fr'
    
    def start_requests(self):
        start_urls = [
            {"url": "https://www.pm-immobilier.fr/location/1", "property_type": "apartment"},
            
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//a[@class='item__title']//@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
            
# 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        status="".join(response.xpath("(//link[@rel='canonical']//@href)[1]").get())
        if "parking" not in status.lower():
            item_loader.add_value("external_source", self.external_source)
            item_loader.add_value("property_type", response.meta.get('property_type'))
            item_loader.add_value("external_link", response.url)

            external_id=response.xpath("//div[@class='table-aria__tr']//span[contains(.,'Code postal')]//following-sibling::span//text()").get()
            if external_id:
                item_loader.add_value("external_id", external_id)

            title=response.xpath("//title//text()").get()
            if title:
                item_loader.add_value("title", title)

            rent=response.xpath("//div[@class='table-aria__tr']//span[contains(.,'Loyer CC* / mois')]//following-sibling::span//text()").get()
            if rent:
                item_loader.add_value("rent", rent.replace(" ",""))
            item_loader.add_value("currency", "EUR")

            square_meters=response.xpath("//div[@class='table-aria__tr']//span[contains(.,'Surface habitable')]//following-sibling::span//text()").get()
            if square_meters:
                meters = square_meters.split('m²')
                item_loader.add_value("square_meters",meters)
            
            room_count=response.xpath("//div[@class='table-aria__tr']//span[contains(.,'Nombre de pièces')]//following-sibling::span//text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count)
            else:
                room_count=response.xpath("//div[@class='table-aria__tr']//span[contains(.,'Nombre de chambre(s)')]//following-sibling::span//text()").get()
                if room_count:
                    item_loader.add_value("room_count", room_count)

            bath_count = response.xpath("//div[@class='table-aria__tr']//span[contains(.,'Nb de salle de bains')]//following-sibling::span//text()").get()
            if bath_count:
                item_loader.add_value("bathroom_count", bath_count)

            city = response.xpath("//div[@class='table-aria__tr']//span[contains(.,'Ville')]//following-sibling::span//text()").get()
            if city:
                item_loader.add_value("city", city.strip())

            desc="".join(response.xpath("//div[@class='description__text-block text-block  ']//p//text()").getall())
            if desc:
                item_loader.add_value("description", desc)
            
            floor=response.xpath("//div[@class='table-aria__tr']//span[contains(.,'Etage')]//following-sibling::span//text()").get()
            if floor:
                item_loader.add_value("floor", floor.strip())
            
            furnished=response.xpath("//div[@class='table-aria__tr']//span[contains(.,'Meublé')]//following-sibling::span//text()").get()
            if furnished:
                if 'NON' in furnished.lower():
                    item_loader.add_value("furnished",False)
                else:
                    item_loader.add_value("furnished",True)
                    
            balcony=response.xpath("//div[@class='table-aria__tr']//span[contains(.,'Balcon')]//following-sibling::span//text()").get()
            if balcony:
                if 'NON' in balcony:
                    item_loader.add_value("balcony",False)
                else:
                    item_loader.add_value("balcony",True)  

            utilties=response.xpath("//div[@class='table-aria__tr']//span[contains(.,'Charges locatives')]//following-sibling::span//text()").get()
            if utilties:
                item_loader.add_value("utilities", utilties.split('€'))
            
            deposit=response.xpath("//div[@class='table-aria__tr']//span[contains(.,'Dépôt de garantie')]//following-sibling::span//text()").get()
            if deposit:
                item_loader.add_value("deposit", deposit.split('€'))
        
            terrace=response.xpath("//div[@class='table-aria__tr']//span[contains(.,'Terrasse')]//following-sibling::span//text()").get()
            if terrace:
                if 'NON' in terrace:
                    item_loader.add_value("terrace",False)
                else:
                    item_loader.add_value("terrace",True)
                
            images=[x for x in response.xpath("//div[contains(@class,'swiper-slide slider-img__swiper-slide')]//a//@href").getall()]
            if images:
                item_loader.add_value("images", images)

            item_loader.add_value("landlord_phone", "04 91 86 44 00")
            item_loader.add_value("landlord_name", "L'AGENCE")
            
            yield item_loader.load_item()