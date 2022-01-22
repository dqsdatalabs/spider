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

class MySpider(Spider):
    name = 'aximmobilier_com'
    execution_type='testing' 
    country='france'
    locale='fr'
    
    def start_requests(self):
        start_urls = [
            {"url": "http://ax-immobilier.com/resultat.php?transac=location&type=appartement&ville=&dateDebut=&nbSemaines=1&budget_mini=0&budget_maxi=100000000&surface_mini=0&surface_maxi=100000&ref_bien=", "property_type": "apartment"},
            {"url": "http://ax-immobilier.com/resultat.php?transac=location&type=maison&ville=&dateDebut=&nbSemaines=1&budget_mini=0&budget_maxi=100000000&surface_mini=0&surface_maxi=100000&ref_bien=", "property_type": "house"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[contains(@class,'annonceImpair')]/div//span[@class='txtContent']//a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
            
# 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Aximmobilier_PySpider_"+ self.country + "_" + self.locale)

        item_loader.add_xpath("title", "//span[contains(@class,'titre')]/text()")
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        
        rent="".join(response.xpath("//span[contains(@class,'titre')]/text()").getall())
        if rent:
            item_loader.add_value("rent_string", rent)
        
        square_meters=response.xpath("//ul[contains(@class,' uldetails')]/li[contains(.,'habitable')]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split(':')[1].split('m²')[0].strip())
        
        room_count=response.xpath("//div/h3[contains(.,'Infos')]//parent::div//text()[contains(.,'chambre') or contains(.,'chambre(s)') or contains(.,'Chambre(s) d')]/following-sibling::b[1]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        
        address=response.xpath("//ul[contains(@class,' uldetails')]/li[contains(.,'Localisation')]/text()").get()
        address=address.split(":")[1].strip()
        if address:
            item_loader.add_value("address", address.replace('(','').replace(')',''))
            item_loader.add_value("zipcode", address.split('(')[1].split(')')[0])
            item_loader.add_value("city", address.split(":")[-1].split("(")[0].strip())

            
        external_id=response.xpath("//ul[contains(@class,' uldetails')]/li[contains(.,'Référence')]/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(':')[1].strip())

        desc="".join(response.xpath("//div/h3[contains(.,'Descriptif')]//parent::div//text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())

            if "garantie" in desc.lower():
                deposit = desc.lower().split("garantie")[1].split("€")[0].replace(":","").strip()
                if deposit:
                    item_loader.add_value("deposit", deposit)
            
        images=[x for x in response.xpath("//ul[@id='thumbs']/li/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        item_loader.add_value("landlord_name","AGENCE AX")
        item_loader.add_value("landlord_phone","05 61 64 24 84")
        
        furnished=response.xpath("//div/h3[contains(.,'Infos')]//parent::div//text()[contains(.,'Meublé')]").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        elevator=response.xpath("//div/h3[contains(.,'Infos')]//parent::div//text()[contains(.,'Ascenseur')]").get()
        if elevator:
            item_loader.add_value("elevator",True)
        
        energy_label=" ".join(response.xpath("//div/h3[contains(.,'Diagnostics')]//parent::div//text()").getall())
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split('énergétique :')[1].split(' ')[0])
        
        bathroom_count = response.xpath("//div/h3[contains(.,'Infos')]//parent::div//text()[contains(.,'bains') or contains(.,'Salle') or contains(.,'salle')]/following-sibling::b[1]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        floor = response.xpath("//div/h3[contains(.,'Infos')]//parent::div//text()[contains(.,'étage')]/following-sibling::b[1]/text()").get()
        if floor:
            item_loader.add_value("floor", floor)
        
        yield item_loader.load_item()