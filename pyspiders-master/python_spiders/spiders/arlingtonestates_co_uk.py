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
import dateparser

class MySpider(Spider):
    name = 'arlingtonestates_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.rightmove.co.uk/student-accommodation/find.html?locationIdentifier=REGION%5E2183&radius=40.0&propertyTypes=flat&primaryDisplayPropertyType=flats&includeLetAgreed=false&mustHave=&dontShow=&furnishTypes=&keywords=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.rightmove.co.uk/student-accommodation/find.html?locationIdentifier=REGION%5E2183&radius=40.0&propertyTypes=bungalow%2Cdetached%2Csemi-detached%2Cterraced&includeLetAgreed=false&mustHave=&dontShow=&furnishTypes=&keywords=",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 24)
        seen=False
        for item in response.xpath("//a[@data-test='property-details']"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})   
            seen=True
        
        if page ==24 or seen:        
            f_url = response.url.replace(f"radius=40.0&", f"radius=40.0&index={page}&")
            yield Request(f_url, callback=self.parse, meta={"page": page+24, "property_type":response.meta["property_type"]})    
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Arlingtonestates_Co_PySpider_united_kingdom")

        rent_string = response.xpath("//div[@class='_1gfnqJ3Vtd1z40MlC0MzXu']/span/text()").get()
        if rent_string:
            item_loader.add_value("rent_string", rent_string.replace(" ", "").replace(',', '').strip().strip('pcm'))
        
    
        desc = "".join(response.xpath("//div[@class='OD0O7FWw1TjbTD4sdRi1_']/div//text()").getall())
        if desc:
            item_loader.add_value("description", re.sub('\s{2,}', ' ', desc))

        script_data = response.xpath("//script[contains(.,'PAGE_MODEL')]/text()").get()
        if script_data:
            data = json.loads(script_data.split("PAGE_MODEL = ")[1].strip())["propertyData"]

            if data and "id" in data:
                item_loader.add_value("external_id", data["id"])
            
            if data and "text" in data and "pageTitle" in data["text"]:
                item_loader.add_value("title", data["text"]["pageTitle"])
            else:
                item_loader.add_value("title", response.meta.get("title"))
            
            if data and "address" in data and "displayAddress" in data["address"]:
                address = data["address"]["displayAddress"]
                item_loader.add_value("address", data["address"]["displayAddress"])
                if "," in address:
                    city = address.split(",")[-1]
                    if city.replace(" ","").isalpha():
                        item_loader.add_value("city", city)
                    else:
                        city = address.split(",")[-2]
                        if city.replace(" ","").isalpha():
                            item_loader.add_value("city", city)

                
            citycheck=item_loader.get_output_value("city")
            if not citycheck: 
                city=response.xpath("//h1[@itemprop='streetAddress']/text()").get()
                if city:
                    item_loader.add_value("city",city.split(",")[1].strip())
            zipcode=response.xpath("//title/text()").get()
            if zipcode:
                item_loader.add_value("zipcode",zipcode.split(",")[-1])

            if data and "location" in data:
                if "latitude" in data["location"] and "longitude" in data["location"]:
                    item_loader.add_value("latitude", str(data["location"]["latitude"]))
                    item_loader.add_value("longitude", str(data["location"]["longitude"]))
            
            if data and "bedrooms" in data:
                item_loader.add_value("room_count", str(data["bedrooms"]))
            else:
                room_count = response.xpath("//div[.='BEDROOMS']/..//div[@class='_1fcftXUEbWfJOJzIUeIHKt']/text()").get()
                if room_count:
                    item_loader.add_value("room_count", room_count.replace("x", "").strip())
            
            if data and "bathrooms" in data:
                item_loader.add_value("bathroom_count", str(data["bathrooms"]))
            else:
                bathroom_count = response.xpath("//div[.='BATHROOMS']/..//div[@class='_1fcftXUEbWfJOJzIUeIHKt']/text()").get()
                if bathroom_count:
                    item_loader.add_value("bathroom_count", bathroom_count.replace("x", "").strip())
            
            if data and "images" in data and len(data["images"]) > 0:
                images = [x["url"] for x in data["images"]]
                item_loader.add_value("images", images)
                item_loader.add_value("external_images_count", len(images))
            
            if data and "floorplans" in data and len(data["floorplans"]) > 0:
                floor_images = [x["url"] for x in data["floorplans"]]
                item_loader.add_value("floor_plan_images", floor_images)
            
            if data and "lettings" in data:
                if "letAvailableDate" in data["lettings"]:
                    available_date = data["lettings"]["letAvailableDate"]
                else:
                    available_date = response.xpath("//span[contains(.,'Let available date')]/following-sibling::*/text()").get()
                if available_date:
                    date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
                
                if "deposit" in data["lettings"]:
                    item_loader.add_value("deposit", str(data["lettings"]["deposit"]))
                
                 
                features=response.xpath("//h2[.='Key features']/following-sibling::ul//li//text()").getall()
                if features:
                    for fea in features:
                        if "furnished" in fea.lower():
                            if "unfurnished" in fea.lower():
                                item_loader.add_value("furnished",False)
                            if "furnished" in fea.lower():
                                item_loader.add_value("furnished",True)
                furnishedcheck=item_loader.get_output_value("furnished")
                if not furnishedcheck:
                    if "furnishType" in data["lettings"] and data["lettings"]["furnishType"] == "Furnished":
                        item_loader.add_value("furnished", True)


        item_loader.add_value("landlord_name", "Arlington Estates")
        item_loader.add_value("landlord_phone", "020 8012 2970")
        item_loader.add_value("landlord_email", "customersupport@rightmove.co.uk")

        yield item_loader.load_item()