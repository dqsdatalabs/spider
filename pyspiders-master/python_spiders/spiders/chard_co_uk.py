# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json


class MySpider(Spider):
    name = 'chard_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    thousand_separator = ','
    scale_separator = '.'       
    def start_requests(self):
        start_urls = [
            {"url": "https://www.chard.co.uk/search.ljson?channel=lettings&fragment=tag-flat/status-available", "property_type": "apartment"},
	        {"url": "https://www.chard.co.uk/search.ljson?channel=lettings&fragment=tag-house/status-available", "property_type": "house"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                        })

    # 1. FOLLOWING
    def parse(self, response):
        property_type = response.meta.get("property_type")
        page = response.meta.get('page', 2)
        data = json.loads(response.body)

        for item in data["properties"]:
            follow_url = response.urljoin(item["property_url"])
            lat = item["lat"]
            lng = item["lng"]
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":property_type, "lat":lat, "lng":lng})

        if data["pagination"]["has_next_page"]:
            base_url = response.meta.get("base_url", response.url)
            url = base_url + f"/page-{page}"
            yield Request(url, callback=self.parse, meta={"page": page+1, "base_url":base_url, "property_type":property_type})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_xpath("title", "concat(//div[contains(@class,'property-details-container')]/h1/text(), ' ', //div[contains(@class,'property-details-container')]/h2/text())")
        
        lat = response.meta.get("lat")
        lng = response.meta.get("lng")
        if lat != 0 and lng != 0 :
            item_loader.add_value("longitude", str(lat))
            item_loader.add_value("latitude",str(lng))
 
        address =", ".join( response.xpath("//div[contains(@class,'property-details-container')]/h1/text() | //div[contains(@class,'property-details-container')]/h2/text()").extract()  )   
        if address:   
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.split(",")[-2].strip())
            item_loader.add_value("zipcode", address.split(",")[-1].strip())

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Chard_Co_PySpider_united_kingdom")
 
        room_count = response.xpath("//div[contains(@class,'property-details-container')]/ul/li[contains(.,'bed')]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split("bed")[0])

        bathroom_count=response.xpath("//div[contains(@class,'property-details-container')]/ul/li[contains(.,'bath')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split("bath")[0])

        rent =" ".join( response.xpath("//div[contains(@class,'property-details-container')]/p[@class='price']//text()[normalize-space()]").extract())
        if rent:
            if "pcm" in rent:
                rent = rent.split("pcm")[0]
            item_loader.add_value("rent_string", rent)    

       
        desc = " ".join(response.xpath("//div[contains(@class,'property-details-container')]/div[@class='property-description']//text()").extract())
        if desc:
            item_loader.add_value("description",desc.strip())
            if "parking" in desc.lower():
                item_loader.add_value("parking",True)
    
        energy = response.xpath("//div[@class='property-epc'][1]//img/@src").extract_first() 
        if energy:            
            energy_label = energy.split('_')[-2]
            if energy_label.isdigit():
                item_loader.add_value("energy_label", energy_label_calculate(energy_label))
            
        img=response.xpath("//div[@id='propertyModalCarousel']//div[@class='slide-inner']/div/@style").extract() 
        if img:
            images=[]
            for x in img:                
                image = x.split('url(')[1].split(')')[0]
                images.append(response.urljoin(image))
            if images:
                item_loader.add_value("images",  list(set(images)))   

        floor_plan_images = [response.urljoin(x) for x in response.xpath("//div[@class='property-floorplan']//img/@src").extract()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)   

        item_loader.add_value("landlord_phone", "020 7384 1400")
        item_loader.add_value("landlord_name", "Chard Estate Agents")
        item_loader.add_value("landlord_email", "fulhamlettings@chard.co.uk")

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