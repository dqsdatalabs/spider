# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from html.parser import HTMLParser
from urllib.parse import urljoin
import math
import re

class MySpider(Spider):
    name = 'swixim_paris_com'
    execution_type='testing'
    country='france'
    locale='fr'
    start_urls = ["https://www.swixim-paris.com/location/1/0"] #LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='properties-grid']/div[contains(@class,'col-xs-12')]"):
            follow_url = response.urljoin(item.xpath(".//a/@href").get())
            prop_type = item.xpath(".//div[@class='location']/div[1]/div[1]/span/text()").get()
            if prop_type and "Appartement" in prop_type:
                prop_type = "apartment"
            elif prop_type and "Maison" in prop_type:
                prop_type = "house"
            else:
                prop_type = None
            yield Request(follow_url, callback=self.populate_item, meta={"prop_type":prop_type})

        next_page = response.xpath("//a[contains(.,'>')]/@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse,
            )


    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        prop_type = response.meta.get('prop_type')        
        prop_type_studio = response.xpath("//div[contains(.,'studio')]/img/parent::div/text()").extract_first()
        if prop_type_studio:
            if "studio" in prop_type_studio:
                prop_type = "studio"
        if prop_type:
            item_loader.add_value("property_type", prop_type)
        else:
            return
        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath("title", "//h1/text()")

        item_loader.add_value("external_source", "Swiximparis_PySpider_"+ self.country + "_" + self.locale)

        latitude_longitude = response.xpath("//script[contains(.,'L.marker')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('L.marker([')[1].split(',')[0]
            longitude = latitude_longitude.split('L.marker([')[1].split(',')[1].split(']')[0]
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
            

        square_meters = response.xpath("//div[contains(.,'m²')]/img/parent::div/text()").get()
        if square_meters:
            square_meters = str(int(float(square_meters.strip().split(' ')[0])))
            item_loader.add_value("square_meters", square_meters)
        
        
        room_count = response.xpath("//div[contains(.,'pièce') or contains(.,'studio')]/img/parent::div/text()").get()
        if "studio" in room_count:
            item_loader.add_value("room_count", "1")
        elif room_count:
            room_count = room_count.strip().split(' ')[0]
            item_loader.add_value("room_count", room_count)
        

        rent = response.xpath("//div[@class='col-md-6 col-xs-12']/h3/text()[1]").get()
        if rent:
            rent = rent.split("€")[0].strip().replace(" ","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")     

        # currency = 'EUR'
        # item_loader.add_value("currency", currency)

        description =" ".join(response.xpath("//div[@class='col-md-6 col-xs-12']//div[1]//text()").getall())    
        if description:            
            item_loader.add_value("description", re.sub('\s{2,}', ' ', description.strip()))
            if "meublé" in description:
                item_loader.add_value("furnished", True)

        if "de garantie" in description:
            deposit = description.split("de garantie :")[1].split("?")[0].strip()
            item_loader.add_value("deposit", deposit)
            
        if "Honoraires :" in description:
            utilities = description.split("Honoraires :")[1].split("?")[0].strip()
            if "," in utilities:
                item_loader.add_value("utilities", utilities.split(",")[0])
            else:
                item_loader.add_value("utilities", utilities)
               
        address = response.xpath("//h2/text()").get()
        if address:
            if "m²" in address and address.split("m²")[1].strip():
                address = address.split("m²")[1].strip()
                if ",1" in address:
                    address = address.split("ième")[1].strip()
                else:
                    address = " ".join(address.split(" - ")[1:]).strip()
                item_loader.add_value("address", address)
            else:
                item_loader.add_value("address"," ".join(address.split(" - ")[1:]).replace("42 ,37m²","").strip())
        
        city = response.xpath("//h1/text()").get()
        if city:
            city = city.split(" à ")[1].strip()
            zipcode = city.split(" ")[-1]
            city = city.split(zipcode)[0].strip()
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)
            
        images = [urljoin('https://www.swixim-paris.com', x) for x in response.xpath("//div[@id='panel_photo']/div[@class='row']//img/@src").getall()]
        if images:
            item_loader.add_value("images", list(set(images)))
            # item_loader.add_value("external_images_count", str(len(images)))

        energy_label = response.xpath("//div[@class='panel-body']/img/parent::div/text()[1]").get()
        if energy_label:
            energy_label = energy_label.split(':')[1].strip()
            if energy_label.isnumeric():
                if int(energy_label) <= 50:
                    energy_label = 'A'
                elif 50 < int(energy_label) and int(energy_label) <= 90:
                    energy_label = 'B'
                elif 90 < int(energy_label) and int(energy_label) <= 150:
                    energy_label = 'C'
                elif 150 < int(energy_label) and int(energy_label) <= 230:
                    energy_label = 'D'
                elif 230 < int(energy_label) and int(energy_label) <= 330:
                    energy_label = 'E'
                elif 330 < int(energy_label) and int(energy_label) <= 450:
                    energy_label = 'F'
                elif 450 < int(energy_label):
                    energy_label = 'G'
                item_loader.add_value("energy_label", energy_label)

        floor = response.xpath("//img[contains(@src,'immeuble')]/parent::div/text()").get()
        if floor:
            floor = floor.split('/')[0].strip()
            item_loader.add_value("floor", floor)

      
        item_loader.add_value("landlord_phone","01 43 48 24 04")
        item_loader.add_value("landlord_name","Swixim")
        
        landlord_email = response.xpath("//div[@class='col-md-6 col-xs-12']//text()[contains(.,'par email')]").get()
        if landlord_email and "contacter" in landlord_email.lower():
            landlord_email = landlord_email.split("par email")[1].replace(":","").replace("-","").strip().split(" ")[0]
            item_loader.add_value("landlord_email", landlord_email)

        bathroom_count = response.xpath("//img[contains(@src,'baignoire') or contains(@src,'douche')]/parent::div/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        elevator = response.xpath("//img[contains(@src,'ascenseur')]/@src").get()
        if elevator:
            item_loader.add_value("elevator", True)
        balcony = response.xpath("//img[contains(@src,'balcon')]/@src").get()
        if balcony:
            item_loader.add_value("balcony", True)
            
        
            
        yield item_loader.load_item()


        
        
 
        
          

        

      
     