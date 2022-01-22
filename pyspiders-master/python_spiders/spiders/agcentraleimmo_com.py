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
    name = 'agcentraleimmo_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source = "Agcentraleimmo_PySpider_france_fr"

    def start_requests(self):
        start_urls = [
            {
                "url" : "https://www.agcentraleimmo.com/recherche/"
            }
            
        ] #LEVEL-1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse)


    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)

        seen = False
        for item in response.xpath("//ul[@class='listingUL']/li"):
            follow_url = response.urljoin(item.xpath(".//div[@class='backBtn']/a/@href").get())
            prop_type = item.xpath(".//div[@class='bienTitle']/h2/text()").get()
            property_type = ""
            if "appartement" in prop_type.lower():
                property_type = "apartment"
            elif "maison" in prop_type.lower():
                property_type = "house"
            elif "studio" in prop_type.lower():
                property_type = "apartment"
            elif "duplex" in prop_type.lower():
                property_type = "apartment"
            elif "villa" in prop_type.lower():
                property_type = "house"
            if property_type != "":
                yield Request(follow_url, callback=self.populate_item, meta={'property_type' : property_type, "f_url":follow_url})
            seen = True
        
        if page == 2 or seen:
            url = f"https://www.agcentraleimmo.com/recherche/{page}"
            yield Request(
                url, 
                callback=self.parse, 
                meta={
                    'property_type' : response.meta.get('property_type'),
                    "page" : page+1
                }
            )
        
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        head_title = response.xpath("//head/title/text()").get()
        if "location" in head_title:
            item_loader.add_value("external_source", self.external_source)
            item_loader.add_value("property_type", response.meta.get('property_type'))
            title = "".join(response.xpath("//div[@class='bienTitle']/h2/text()").extract())
            item_loader.add_value("title", re.sub("\s{2,}", " ", title))
            item_loader.add_value("external_link", response.meta.get("f_url"))

            
            rent="".join(response.xpath("//div[@id='infosfi']/p[contains(.,'mois')]/span[2]/text()").getall())
            if rent:
                rent = rent.replace("€", "").replace(" ","")
                item_loader.add_value("rent", rent)
            item_loader.add_value("currency", "EUR")
            
            square_meters=response.xpath("//div[@id='dataContent']/div/p[contains(.,'habitable')]/span[2]/text()").get()
            if square_meters:
                square_meters = square_meters.split('m²')[0].strip()
                if "," in square_meters:
                    square_meters = square_meters.replace(",", ".")
                square_meters = math.ceil(float(square_meters))
                item_loader.add_value("square_meters", str(square_meters))
            
            room_count=response.xpath("//span[@class='termInfos'][contains(text(),'pièces')]/following-sibling::span/text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count)
            
            latitude_longitude = response.xpath("//script[contains(.,'lat')]/text()").get()
            if latitude_longitude:
                latitude = latitude_longitude.split('lat :')[1].split(',')[0].strip()
                longitude = latitude_longitude.split('lng:')[1].split('}')[0].strip()
                if latitude and longitude:
                    item_loader.add_value("longitude", longitude)
                    item_loader.add_value("latitude", latitude)           

            zipcode = response.xpath("normalize-space(//span[contains(.,'Code postal')]/following-sibling::*/text())").get()
            if zipcode:
                item_loader.add_value("zipcode", zipcode)
            
            address = ""
            address = "".join(response.xpath("//ol[@class='breadcrumb']/li[2]//text()").getall()) 
            if address:
                item_loader.add_value("address", address)

            city = response.xpath("//span[.='Ville']/following-sibling::span/text()").get()
            if city:
                item_loader.add_value("city", city.strip())
            else:
                item_loader.add_value("city", address)
                     
            utilties = response.xpath("normalize-space(//span[contains(.,'charge locataire')]/following-sibling::span/text())").get()
            if utilties:
                item_loader.add_value("utilities", utilties.replace("€","").strip()) 
            # address_list = title.split("-")
            # for item in address_list:
            #     if zipcode in item:
            #         address = item.split("(")[0].strip()
            #         break
            

            bathroom_count = response.xpath("normalize-space(//div[@id='dataContent']/div/p[contains(.,'salle ')]/span[2]/text())").get()
            if bathroom_count:
                item_loader.add_value("bathroom_count", bathroom_count)
                
                 
            external_id=response.xpath("//li[@class='ref']//text()[contains(.,'Ref ')]").get()
            if external_id:
                item_loader.add_value("external_id", external_id.split("Ref ")[1].replace("\n",""))

            desc="".join(response.xpath("//p[@itemprop='description']//text()").getall())
            if desc:
                item_loader.add_value("description", desc.strip())
                
            images=[x for x in response.xpath("//ul[contains(@class,'imageGallery')]/li/img/@src").getall()]
            if images:
                item_loader.add_value("images", images)
                item_loader.add_value("external_images_count", str(len(images)))
            
            item_loader.add_value("landlord_name","ARTHURIMMO.COM")
            item_loader.add_value("landlord_phone","03 23 96 05 93")
            item_loader.add_value("landlord_email","arthurimmobilier@gmail.com")
            
            floor=response.xpath("//div[@id='dataContent']/div/p[contains(.,'Etage')]/span[2]/text()").get()
            if floor:
                item_loader.add_value("floor", floor.strip())
                
            balcony=response.xpath("//div[@id='details']/p[contains(.,'Balcon')]/span[2]/text()").get()
            if balcony and "OUI" in balcony:
                item_loader.add_value("balcony",True)
            elif balcony and "OUI" not in balcony:
                item_loader.add_value("balcony",False)

            parking=response.xpath("normalize-space(//div[@id='details']/p[contains(.,'garage')]/span[2]/text())").get()
            if parking:
                if "non" in parking.lower():
                    item_loader.add_value("parking",False)
                else:
                    item_loader.add_value("parking",True)
            
            furnished=response.xpath("//div[@id='dataContent']/div/p[contains(.,'Meublé')]/span[2]/text()").get()
            if furnished and "OUI" in furnished:
                item_loader.add_value("furnished",True)
            elif furnished and "OUI" not in furnished:
                item_loader.add_value("furnished",False)
                
            utilties=response.xpath("//div[@id='infosfi']/p[contains(.,'Charge')]/span[2]/text()").get()
            if utilties:
                item_loader.add_value("utilities", utilties.split('€')[0].strip())
            
            deposit=response.xpath("//div[@id='infosfi']/p[contains(.,'garantie')]/span[2]/text()").get()
            if deposit:
                item_loader.add_value("deposit", deposit.split('€')[0].strip())
            
            elevator=response.xpath("//div[@id='dataContent']/div/p[contains(.,'Ascenseur')]/span[2]/text()").get()
            if elevator and "OUI" in elevator:
                item_loader.add_value("elevator",True)
            elif elevator and "OUI" not in elevator:
                item_loader.add_value("elevator",False)
            
            terrace=response.xpath("//div[@id='details']/p[contains(.,'Terrasse')]/span[2]/text()").get()
            if terrace and "OUI" in terrace:
                item_loader.add_value("terrace",True)
            elif terrace and "OUI" not in terrace:
                item_loader.add_value("terrace",False)
                
            yield item_loader.load_item()