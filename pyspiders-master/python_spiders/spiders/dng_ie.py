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
    name = 'dng_ie'
    execution_type='testing'
    country='ireland'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.dng.ie/rentals/ireland/apartment-to-rent",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.dng.ie/rentals/results?types=75%7C76%7C77%7C78%7C79%7C99%7C106&status=11",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://www.dng.ie/rentals/ireland/studio-to-rent",
                ],
                "property_type" : "studio",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//a[contains(.,'View Details')]/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_button = response.xpath("//a[contains(.,'»')]/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("/")[-1])
        item_loader.add_value("external_source", "Dng_PySpider_ireland")
        item_loader.add_xpath("title","//div[@class='row']//h2/text()[normalize-space()]")
        # short_term = response.xpath("//div[@class='row']//h2/text()[contains(.,' month lease')]").get()
        # if short_term:       
        #     return
        zipcode = response.xpath("//div[h3[.='Directions']]//text()[contains(.,'Eircode')]").get()
        if zipcode:                   
            zipcode = zipcode.split("Eircode")[1].replace(':','').strip()
        address = "".join(response.xpath("//div[@class='row']/div/h1/text()").getall())
        if address:
            item_loader.add_value("address", address.strip())
            city = address.split(",")[-1].replace("Co.","").strip().split(" ")[0]
            if city.isalpha():
                item_loader.add_value("city", city)
            else:
                city = address.split(",")[-2].strip()
                item_loader.add_value("city", city)

            if not zipcode:
                address = address.replace(",","").strip().split(" ")
                if not address[-2].isalpha() and not address[-1].isalpha():
                    zipcode = f"{address[-2]} {address[-1]}"
                elif not address[-1].isalpha():
                    zipcode = address[-1]
        
        if zipcode:
            item_loader.add_value("zipcode", zipcode.split(" ")[-1])
        
        room_count = response.xpath("//div[@class='row']//h2/text()[contains(.,'Bed ')]").get()
        if room_count:                   
            item_loader.add_value("room_count",room_count.split("Bed ")[0].strip().split(" ")[-1].strip())
        square_meters = response.xpath("//div[@class='row']//h2/text()[contains(.,'m²')]").get()
        if square_meters:               
            square_meters = square_meters.split("m²")[0].strip().split(" ")[-1].split(".")[0].strip()    
            item_loader.add_value("square_meters", square_meters)
        bathroom_count = response.xpath("//div[h3[.='Features'] or h3[.='Summary']]//text()[contains(.,'Bathroom')]").get()
        if bathroom_count:                   
            item_loader.add_value("bathroom_count",bathroom_count.split("Bathroom")[0].strip().split(" ")[0].strip())
        rent = response.xpath("//div[@class='row']/div/h1/span[contains(.,'€')]/text()").get()
        if rent:       
            if "week" in rent:
                rent = rent.split('€')[-1].split('/')[0].strip().replace(',', '').replace('\xa0', '')
                item_loader.add_value("rent", str(int(float(rent)) * 4))
            else:       
                rent = rent.split('€')[-1].split('/')[0].strip().replace(',', '').replace('\xa0', '')
                item_loader.add_value("rent", str(int(float(rent))))

        item_loader.add_value("currency", 'EUR')
        item_loader.add_xpath("latitude", "//input[@id='propertyLat']/@value") 
        item_loader.add_xpath("longitude", "//input[@id='propertyLng']/@value") 
    
        description = " ".join(response.xpath("//div[@class='propertyContent'][h3[.='Description']]/p//text()").getall())  
        if description:
            item_loader.add_value("description", description.strip())

        images = [response.urljoin(x) for x in response.xpath("//div[@class='row']//a[contains(@class,'swipebox')]/@href").getall()]
        if images:
            item_loader.add_value("images", images)
        energy_label = response.xpath("//h2/img[@class='BER']/@alt").get()
        if energy_label:                   
            item_loader.add_value("energy_label",energy_label.strip())
        furnished = response.xpath("//div[h3[.='Features'] or h3[.='Summary']]//text()[contains(.,'Furnished') or contains(.,'furnished')]").get()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished",False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished",True)
        parking = response.xpath("//div[h3[.='Features'] or h3[.='Summary']]//text()[contains(.,'Parking')]").get()
        if parking:                   
            item_loader.add_value("parking", True)
        balcony = response.xpath("//div[h3[.='Features'] or h3[.='Summary']]//text()[contains(.,'Balcony')]").get()
        if balcony:                   
            item_loader.add_value("balcony", True)
        washing_machine = response.xpath("//div[h3[.='Features'] or h3[.='Summary']]//text()[contains(.,'Washing Machine')]").get()
        if washing_machine:                   
            item_loader.add_value("washing_machine", True)
        dishwasher = response.xpath("//div[h3[.='Features'] or h3[.='Summary']]//text()[contains(.,'Dishwasher')]").get()
        if dishwasher:                   
            item_loader.add_value("dishwasher", True)
 
        item_loader.add_xpath("landlord_name", "//li[@id='AgentName']/text()")
        landlord_phone = response.xpath("//li[@id='AgentPhone']/text()").get()
        if landlord_phone:                   
            item_loader.add_value("landlord_phone", landlord_phone.replace("Call","").strip())
        yield item_loader.load_item()