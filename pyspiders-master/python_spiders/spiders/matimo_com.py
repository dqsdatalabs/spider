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
    name = 'matimo_com'
    execution_type='testing'
    country='france'
    locale='fr'
    post_url = "https://www.matimo.com/fr/recherche/"
    current_index = 0
    other_prop = ["2"]
    other_type = ["house"]
    def start_requests(self):
        formdata = {
            "nature": "2",
            "type": "1",
            "sector": "",
            "price": "0",
        }
        yield FormRequest(self.post_url,
                        callback=self.parse,
                        formdata=formdata,
                        dont_filter=True,
                        meta={'property_type': "apartment"})

            
    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for url in response.xpath("//a[@class='more']/@href").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True
        if page == 2 or seen:            
            p_url = f"https://www.matimo.com/fr/recherche/{page}"
            yield Request(
                p_url,
                callback=self.parse,
                dont_filter=True,
                meta={"page":page+1, "property_type":response.meta["property_type"]})
        elif self.current_index < len(self.other_prop):
            formdata = {
                "nature": "2",
                "type": self.other_prop[self.current_index],
                "sector": "",
                "price": "0",
            }
            yield FormRequest(self.post_url,
                            callback=self.parse,
                            formdata=formdata,
                            dont_filter=True,
                            meta={'property_type': self.other_type[self.current_index],})
            self.current_index += 1

                
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_source", "Matimo_PySpider_france")
        
        title = "".join(response.xpath("//span[@class='block']/text()").getall())
        if title:
            item_loader.add_value("title", title.replace(">","").strip())
        
        address = response.xpath("//h1/text()").get()
        item_loader.add_value("address", address)
        item_loader.add_value("city", "PARIS")
        
        rent = response.xpath("//p[@class='price']/text()").get()
        if rent:
            price = rent.split("€")[0].replace(" ","")
            item_loader.add_value("rent", price)
        item_loader.add_value("currency", "EUR")
        
        room_square = response.xpath("//div[@class='blockContent']/h2/text()").get()
        if "chambre" in room_square:
            item_loader.add_value("room_count", room_square.split("chambre")[0].strip().split(" ")[-1])
        elif "pièce" in room_square:
            item_loader.add_value("room_count", room_square.split("pièce")[0].strip().split(" ")[-1])
        
        if "m²" in room_square:
            item_loader.add_value("square_meters", room_square.split("m²")[0].strip().split(" ")[-1])
        
        external_id = response.xpath("//p//text()[contains(.,'Référence')]").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split("—")[0].strip().split(" ")[-1])
            
        fees = response.xpath("//p[@class='fees']//text()").get()
        if "garantie" in fees:
            deposit = fees.split("garantie :")[1].split("€")[0].replace(" ","")
            item_loader.add_value("deposit", deposit)
        if "Charges" in fees:
            utilities = fees.split("Charges :")[1].split("€")[0].replace(" ","")
            item_loader.add_value("utilities", utilities)

        feautures = response.xpath("//p[contains(.,'Prestations :')]/text()").get()
        if feautures:
            if "Meublé" in feautures:
                item_loader.add_value("furnished", True)
            if "Lave-vaisselle" in feautures:
                item_loader.add_value("dishwasher", True)
            if "Lave-linge" in feautures:
                item_loader.add_value("dishwasher", True)
            if "Ascenseur" in feautures:
                item_loader.add_value("elevator", True)
        furnished = response.xpath("//p/text()[contains(.,'meublé')]").get()
        if furnished:
            item_loader.add_value("furnished", True)
                    
        description = " ".join(response.xpath("//div[@class='blockContent']/h2/following-sibling::p//text()").getall())
        if description:
            description = re.sub('\s{2,}', ' ', description.strip())
            item_loader.add_value("description", description)
        
        images = [x for x in response.xpath("//div[@id='pictureSlider']//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        latitude_longitude = response.xpath("//script[contains(.,'L.marker([')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('L.marker([')[1].split(',')[0]
            longitude = latitude_longitude.split('L.marker([')[1].split(',')[1].split(']')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        item_loader.add_value("landlord_name", "MATIMO")
        item_loader.add_value("landlord_phone", "33 0 1 42 72 33 25")
        item_loader.add_value("landlord_email", "contact@matimo.com")
        
        yield item_loader.load_item()