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
import re
from word2number import w2n

class MySpider(Spider):
    name = 'regentsresidential_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    def start_requests(self):

        start_urls = [
            {
                "type" : 1,
                "x" : "25",
                "y" : "14",
                "property_type" : "house"
            },
            {
        
                "type" : 0,
                "x" : "31",
                "y" : "14",
                "property_type" : "apartment"
            },
            
        ] #LEVEL-1

        for url in start_urls:
            r_type = str(url.get("type"))
            payload = {
                "perpage": "99999999",
                "sortDescending": "true",
                "rentalPeriod": "3",
                "minPrice": "0",
                "maxPrice": "99999999",
                "bedrooms": "0",
                "propertyTypeSelect": r_type,
                "searchLocationCodes": "",
                "x": url.get("x"),
                "y": url.get("y"),
                "propertyType": r_type,
                "classification": "",
            }

            yield FormRequest(url="https://www.regentsresidential.com/properties/results.php",
                                callback=self.parse,
                                formdata=payload,
                                #headers=self.headers,
                                meta={'property_type': url.get('property_type')})
            
    # 1. FOLLOWING
    def parse(self, response): 
        for item in response.xpath("//li[@class='propertyItem']"):
            url = item.xpath(".//a[@class='propertyImage']/@href").get()
            rent = item.xpath(".//div[contains(@class,'price')]//text()").get()
            f_url = response.urljoin(url)
            yield Request(
                f_url, 
                callback=self.populate_item, 
                meta={"property_type" : response.meta.get("property_type"), "rent" : rent},
            )
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url.split("&session")[0])
        item_loader.add_value("external_source", "Regentsresidential_PySpider_" + self.country + "_" + self.locale)

        desc = "".join(response.xpath("//div[@class='DescriptionMain']//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip()) 
            if "TERRACE" in desc:
                item_loader.add_value("terrace", True) 
            if "PARKING" in desc:
                item_loader.add_value("parking", True) 
            if "BALCONY" in desc:
                item_loader.add_value("balcony", True) 
            if "SWIMMING POOL" in desc:
                item_loader.add_value("swimming_pool", True) 

            desc_sq=desc.replace("(","").replace(")","")
            unit_pattern = re.findall(r"[+-]? *((?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)\s*(m²|meters2|metres2|meter2|metre2|mt2|m2|M2|sqm|sq m)",desc_sq.replace(",",""))
            if unit_pattern:
                sqm = str(int(float(unit_pattern[0][0])))
                item_loader.add_value("square_meters", sqm)
            else:
                unit_pattern = re.findall(r"[+-]? *((?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)\s*(sqft|sq ft|m²|meters2|metres2|meter2|metre2|mt2|m2|M2|sq.|sq)",desc_sq.replace(",",""))
                if unit_pattern:
                    sqm = str(int(float(unit_pattern[0][0]) * 0.09290304))
                    item_loader.add_value("square_meters", sqm)
        
        if "unfurnished" in desc.lower():
            item_loader.add_value("furnished", False)
        elif "furnished" in desc.lower():
            item_loader.add_value("furnished", True)
        
        title = response.xpath("//div[@class='address']/text()").extract_first()
        if title:
            item_loader.add_value("title", title)
            item_loader.add_value("address",title)
            item_loader.add_value("zipcode",title.split("\xa0")[-1])

        rent = response.meta.get("rent")
        if rent:
            if "week" in rent:
                rent = rent.split('£')[-1].split('per')[0].strip().replace(',', '').replace('\xa0', '').replace("\t","").replace("\n","").replace("\r","")
                item_loader.add_value("rent", str(int(float(rent)) * 4))
                item_loader.add_value("currency", 'GBP')
            elif "day" in rent:
                rent = rent.split('£')[-1].split('per')[0].strip().replace(',', '').replace('\xa0', '').replace("\t","").replace("\n","").replace("\r","")
                item_loader.add_value("rent", str(int(float(rent)) * 30))
                item_loader.add_value("currency", 'GBP')
            else:
                item_loader.add_value("rent_string", rent.replace(",","."))    
    
        room_count = response.xpath("//li[contains(.,'bedroom')]//text()[not(contains(.,'0'))]").extract_first()
        if room_count:
            item_loader.add_value("room_count", room_count.split("bedroom")[0].strip())
        elif "studio" in desc.lower():
            item_loader.add_value("room_count","1")
            item_loader.add_value("property_type","studio")
        
        bathroom=response.xpath("//li[contains(.,'bathroom')]//text()[not(contains(.,'0'))]").get()
        if bathroom:
            item_loader.add_value("bathroom_count", bathroom.split("bathroom")[0].strip())
            
        item_loader.add_value("property_type", response.meta.get('property_type'))
        
        external_id = response.xpath("//div[@class='callBox']//text()").extract_first()
        if external_id:
            item_loader.add_value("external_id", external_id.split("ref:")[1].strip())       
  
        latitude=response.xpath("//span[@id='lat']/text()").get()
        longitude=response.xpath("//span[@id='long']/text()").get()
        item_loader.add_value("latitude",latitude)
        item_loader.add_value("longitude",longitude)
        
        images = [x for x in response.xpath("//table//a/img[contains(@class,'PICTURE')]/@src").extract()]
        if images:
            item_loader.add_value("images", images)
        floor_plan_images = [x for x in response.xpath("//div[@class='floorplanImg']/img/@src").extract()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        lift = "".join(response.xpath("//div[@class='feature']/p/text()[contains(.,'LIFT')]").extract())
        if lift :
            item_loader.add_value("elevator", True) 
        else:
            item_loader.add_value("elevator", False) 
        
        energy_l=response.xpath("//td/div[contains(.,'Energy')]/parent::td/img/@src").get()
        if energy_l:
            energy_label=energy_l.split("Current=")[1].split("&")[0]
            if energy_label.isdigit():
                item_loader.add_value("energy_label", energy(int(energy_label)))
        
        if "floor" in desc.lower():
            floor=desc.lower().split("floor")[0].strip().split(" ")[-1]
            if floor_trans(floor):
                item_loader.add_value("floor", floor_trans(floor))
                                      
        item_loader.add_value("landlord_email", "info@regentsresidential.com")
        item_loader.add_value("landlord_name", "Regents Residential")
        item_loader.add_value("landlord_phone", "02073723000")
        yield item_loader.load_item()

def energy(energy_label):
    if energy_label > 0:
        if energy_label <= 50:
            energy_label = 'A'
        elif energy_label >= 51 and energy_label <= 90:
            energy_label = 'B'
        elif energy_label >= 91 and energy_label <= 150:
            energy_label = 'C'
        elif energy_label >= 151 and energy_label <= 230:
            energy_label = 'D'
        elif energy_label >= 231 and energy_label <= 330:
            energy_label = 'E'
        elif energy_label >= 331 and energy_label <= 450:
            energy_label = 'F'
        elif energy_label >= 451:
            energy_label = 'G'
        return energy_label

def floor_trans(floor):
    
    if floor.replace("rd","").replace("th","").replace("nd","").replace("st","").isdigit():
        floor = floor.replace("rd","").replace("th","").replace("nd","").replace("st","")
    elif ("patiowood" in floor) or ("wood" in floor.lower()) or ("tiled" in floor):
        floor = False
    elif ("refurbishedwooden" in floor) or ("timber" in floor) or ("garden" in floor) or ("with" in floor):
        floor = False
    else:
        return floor.replace("-","").replace("s.","")
