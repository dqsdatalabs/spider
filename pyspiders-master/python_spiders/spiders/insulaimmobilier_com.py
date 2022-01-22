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


class MySpider(Spider):
    name = 'insulaimmobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'

    def start_requests(self):
        start_urls = [
            {
                "url" : "https://www.insula-immobilier.com/recherche/"
            }
            
        ] #LEVEL-1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse)


    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)

        seen = False
        for item in response.xpath("//div[@class='selection-good']/article"):
            follow_url = response.urljoin(item.xpath(".//a[contains(.,'Détails')]/@href").get())
            prop_type = item.xpath(".//h2/text()").get()
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
            elif "immeuble" in prop_type.lower():
                property_type = "house"
            if property_type != "":
                yield Request(follow_url, callback=self.populate_item, meta={'property_type' : property_type})
            seen = True
        
        if page == 2 or seen:
            url = f"https://www.insula-immobilier.com/recherche/{page}"
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

        detail_head = response.xpath("//head/title/text()").get()
        if "location" in detail_head.lower():
            item_loader.add_value("property_type", response.meta.get('property_type'))
            item_loader.add_xpath("title", "//div[contains(@class,'title-Detail')]/h1/text()")
            item_loader.add_value("external_source", "Insulaimmobilier_PySpider_" + self.country + "_" + self.locale)
            item_loader.add_value("external_link", response.url)
            
            external_id = response.xpath("//p[@class='ref']//text()").extract_first()
            if external_id:
                item_loader.add_value("external_id", external_id.split(":")[1].strip())

            item_loader.add_xpath("title", "//div[contains(@class,'title-Detail')]/h1//text()")

            price = response.xpath("//p[@class='price']//text()").extract_first()
            if price:
                item_loader.add_value("rent_string", price.replace(" ","."))

            bath_count = response.xpath("substring-after(//ul/li[contains(.,'Nb de salle d')]/text(),':')").get()
            if bath_count:
                item_loader.add_value("bathroom_count", bath_count.strip())
    
            room_count = response.xpath("//ul/li[contains(.,'Nombre de pièces')]//text()").extract_first()
            if room_count:
                item_loader.add_value("room_count", room_count.split(":")[1].strip())
            floor = response.xpath("//ul/li[contains(.,'Etage')]//text()").extract_first()
            if floor:
                item_loader.add_value("floor", floor.split(":")[1].strip() )
        
            square = response.xpath("//ul/li[contains(.,'Surface')]//text()").extract_first()
            if square:
                square_meters = square.split(":")[1].split("m")[0].strip()
                sq_meters = math.ceil(float(square_meters.replace(",",".")))
                item_loader.add_value("square_meters", sq_meters)
            
            desc = "".join(response.xpath("//p[@itemprop='description']//text()").extract())
            if desc:
                item_loader.add_value("description", desc.strip())

            city = response.xpath("//ul/li[contains(.,'Ville')]/span/text()").extract_first()
            if city:     
                item_loader.add_value("city", city.strip())
                item_loader.add_value("address", city.strip())

            zipcode = response.xpath("//ul/li[contains(.,'Code posta')]//text()").extract_first()
            if zipcode:
                item_loader.add_value("zipcode", zipcode.split(":")[1].strip() )
                
            deposit = response.xpath("//ul/li[contains(.,'Dépôt de garantie') and not(contains(.,'Non'))]//text()").extract_first()
            if deposit:
                item_loader.add_value("deposit", deposit.split(":")[1].split("€")[0].replace(" ","").strip())

            utilities = response.xpath("//ul/li[contains(.,'Charges')]//text()").extract_first()
            if utilities:
                item_loader.add_value("utilities", utilities.split(":")[1].split("€")[0].strip())
            
            furnished = response.xpath("//ul/li[contains(.,'Meublé ')]//text()").extract_first()
            if furnished:
                if "NON" in furnished.upper():
                    item_loader.add_value("furnished", False)
                else:
                    item_loader.add_value("furnished", True)
            elevator = response.xpath("//ul/li[contains(.,'Ascenseur')]//text()").extract_first()
            if elevator:
                if "NON" in elevator:
                    item_loader.add_value("elevator", False)
                else:
                    item_loader.add_value("elevator", True)

            latitude_longitude = response.xpath("//script[contains(.,'center:')]/text()").get()
            if latitude_longitude:
                latitude = latitude_longitude.split('lat :')[1].split(',')[0].strip()
                longitude = latitude_longitude.split('lng:')[1].split('}')[0].strip()
                if latitude or longitude:
                    item_loader.add_value("latitude", latitude)
                    item_loader.add_value("longitude", longitude)
        
            parking = response.xpath("//ul/li[contains(.,'Parking')]//text()").extract_first()
            if parking:
                item_loader.add_value("parking", True)

            images = [response.urljoin(x) for x in response.xpath("//div[@class='slider']//li/img/@src").extract()]
            if images is not None:
                item_loader.add_value("images", images)      

            item_loader.add_value("landlord_email", "info@insula-immobilier.com")
            item_loader.add_value("landlord_name", "insula immobilie")
            yield item_loader.load_item()