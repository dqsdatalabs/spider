# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
 

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
import re

class MySpider(Spider):
    name = 'nazemi_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    thousand_separator = ','
    scale_separator = '.'
    def start_requests(self):
        start_urls = [
            {"url": "https://www.nazemi.co.uk/results?searchurl=%2fresults&market=1&ccode=UK&pricetype=3&proptype=Flat&statustype=4&offset=0", "property_type": "apartment"},
	        {"url": "https://www.nazemi.co.uk/results?searchurl=%2fresults&market=1&ccode=UK&pricetype=3&proptype=House&statustype=4&offset=0", "property_type": "house"},
            
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                        })

    # 1. FOLLOWING
    def parse(self, response):
        property_type = response.meta.get("property_type")

        for item in response.xpath("//div[@id='propertyresults']/div[contains(@class,'results-list')]"):
            url =item.xpath(".//a[contains(@class,'more-info')]/@href").extract_first()
            follow_url = response.urljoin(url)
            room = item.xpath(".//span[i[@class='icon-bed']]/text()[normalize-space()]").extract_first()
            bathroom = item.xpath(".//span[i[@class='icon-bath']]/text()[normalize-space()]").extract_first()
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":property_type,"room":room,"bathroom":bathroom})
        pagination = response.xpath("//ul[@class='pagination']/li/a[contains(.,'Next')]/@href").get()
        if pagination:
            yield Request(response.urljoin(pagination), callback=self.parse, meta={"property_type":property_type})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        studio_control = " ".join(response.xpath("//section[@class='detail-content']/header//h2//text()").getall()).strip()
        if 'studio' in studio_control.lower():
            item_loader.add_value("property_type", 'studio')
        else:
            item_loader.add_value("property_type", response.meta.get('property_type'))

        item_loader.add_value("room_count", response.meta.get('room'))
        item_loader.add_value("bathroom_count", response.meta.get('bathroom'))
        item_loader.add_value("external_link", response.url)
        rented = response.xpath("//h1//span[contains(.,'Let Agreed')]/text()").extract_first()
        if rented:   
            return
        title = " ".join(response.xpath("//h1/a//text()").extract())
        if title:
            item_loader.add_value("title", title.strip())
        item_loader.add_value("external_source", "Nazemi_Co_PySpider_united_kingdom")
        address = response.xpath("//h1/a/text()").extract_first()
        if address:
            address = address.strip().replace(",,",",").replace("- ","")
            item_loader.add_value("address", address.strip())
            item_loader.add_value("zipcode", address.split(",")[-1].strip())
            item_loader.add_value("city", address.split(",")[-2].strip())
        
        rent = response.xpath("//h2//span[@class='priceask']/text()").extract_first()
        if rent:    
            if "pw" in rent:
                rent = rent.split('£')[1].split('pw')[0].strip().replace(',', '').replace('\xa0', '')
                item_loader.add_value("rent", str(int(float(rent)) * 4))
                item_loader.add_value("currency", 'GBP')
            else:
                item_loader.add_value("rent_string", rent)    
     
        desc = " ".join(response.xpath("//section[@class='detail-content']/p//text()").extract())
        if desc:
            item_loader.add_value("description",desc.strip())      
            unit_pattern = re.findall(r"[+-]? *((?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)\s*(Sq. Ft.|sq. ft.|Sq. ft.|sq. Ft.|sq|Sq)",desc.replace(",",""))
            if unit_pattern:
                square_title=unit_pattern[0][0]
                sqm = str(int(float(square_title) * 0.09290304))
                item_loader.add_value("square_meters", sqm)
      
        available_date = response.xpath("//span[@class='available-date']//text()").extract_first()             
        if available_date:
            try:
                date_parsed = dateparser.parse(available_date.lower().split("available from")[1].split(".")[0], languages=['en'])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)      
            except:
                pass       
        images = [response.urljoin(x) for x in response.xpath("//div[@id='slides']//ul/li/img/@src").extract()]
        if images:
            item_loader.add_value("images", images)   

        floor_plan_images = [response.urljoin(x) for x in response.xpath("//div[@id='floorplan-slider']//img/@src").extract()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        map_coordinate = response.xpath("//script//text()[contains(.,'google.maps.LatLng(')]").extract_first()
        if map_coordinate:
            map_coordinate = map_coordinate.split("google.maps.LatLng(")[1].split("),")[0].strip()
            latitude = map_coordinate.split(",")[0].strip()
            longitude = map_coordinate.split(",")[1].strip()          
            if latitude and longitude:
                item_loader.add_value("longitude", longitude)
                item_loader.add_value("latitude", latitude)
        
        energy = response.xpath("//table[@class='epcTab']//td[@class='epcPotential']/img[contains(@src,'energy')]/@src").extract_first()
        if energy:
            energy_label = energy.split("/")[-1].split(".")[0].strip()
            if energy_label.isdigit():
                item_loader.add_value("energy_label", energy_label_calculate(energy_label)) 

        balcony = response.xpath("//div[@class='bullets-li']/p[contains(.,'balcony') or contains(.,'Balcony')]//text()").extract_first()
        if balcony: 
            item_loader.add_value("balcony", True) 
        terrace = response.xpath("//div[@class='bullets-li']/p[contains(.,'terrace') or contains(.,'Terrace')]//text()").extract_first()
        if terrace: 
            item_loader.add_value("terrace", True) 
        parking = response.xpath("//div[@class='bullets-li']/p[contains(.,'park') or contains(.,'Park')]//text()").extract_first()
        if parking: 
            item_loader.add_value("parking", True) 

        elevator = response.xpath("//div[@class='bullets-li']/p[contains(.,'Lift')]//text()").extract_first()
        if elevator: 
            item_loader.add_value("elevator", True) 

        furnished = response.xpath("//div[@class='bullets-li']/p[contains(.,'Furnished') or contains(.,'furnished')]//text()").extract_first()
        if furnished: 
            if "furnished or unfurnished" in furnished.lower():
                pass
            elif "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)  
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished", True) 
      
        item_loader.add_value("landlord_email", "info@nazemi.co.uk")
        item_loader.add_value("landlord_phone", "02077232393")
        item_loader.add_value("landlord_name", "Nazemi")
        yield item_loader.load_item()
def energy_label_calculate(energy_number):
    energy_number = int(energy_number)
    energy_label = ""
    if energy_number <= 20:
        energy_label = "G"
    elif energy_number > 20 and energy_number <= 38:
        energy_label = "F"
    elif energy_number > 38 and energy_number <= 54:
        energy_label = "E"
    elif energy_number > 54 and energy_number <= 68:
        energy_label = "D"
    elif energy_number > 68 and energy_number <= 80:
        energy_label = "C"
    elif energy_number > 80 and energy_number <= 91:
        energy_label = "B"
    elif energy_number > 91:
        energy_label = "A"
    return energy_label