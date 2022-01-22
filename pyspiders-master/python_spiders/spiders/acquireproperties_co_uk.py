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
from datetime import datetime
from datetime import date
import dateparser

class MySpider(Spider):
    name = 'acquireproperties_co_uk'    
    execution_type='testing'
    country='united_kingdom'
    locale='en'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "http://acquireproperties.co.uk/property-search?feature=Apartment&is-buy=false&show-let-agreed=false",
                    "http://acquireproperties.co.uk/property-search?feature=Ground%20Floor%20Flat&is-buy=false&show-let-agreed=false",
                    "http://acquireproperties.co.uk/property-search?feature=Flat&is-buy=false&show-let-agreed=false",
                ],
                "property_type" : "apartment"
            },
            {
                "url" : [
                    "http://acquireproperties.co.uk/property-search?feature=Town%20House&is-buy=false&show-let-agreed=false",
                    "http://acquireproperties.co.uk/property-search?feature=Mid%20Terraced%20House&is-buy=false&show-let-agreed=false",
                    "http://acquireproperties.co.uk/property-search?feature=Semi%20Detached%20House&is-buy=false&show-let-agreed=false",
                    "http://acquireproperties.co.uk/property-search?feature=Terrace%20House&is-buy=false&show-let-agreed=false",
                    "http://acquireproperties.co.uk/property-search?feature=Maisonette&is-buy=false&show-let-agreed=false",
                    "http://acquireproperties.co.uk/property-search?feature=Terraced&is-buy=false&show-let-agreed=false",
                    "http://acquireproperties.co.uk/property-search?feature=Semi-Detached%20House&is-buy=false&show-let-agreed=false",
                    "http://acquireproperties.co.uk/property-search?feature=Family%20Home&is-buy=false&show-let-agreed=false",
                    "http://acquireproperties.co.uk/property-search?feature=Detached%20House&is-buy=false&show-let-agreed=false",
                    "http://acquireproperties.co.uk/property-search?feature=End%20Terraced%20House&is-buy=false&show-let-agreed=false",

                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "http://acquireproperties.co.uk/property-search?feature=Student%20Property&is-buy=false&show-let-agreed=false",
                    
                ],
                "property_type" : "student_apartment"
            },   
            {
                "url" : [
                    "http://acquireproperties.co.uk/property-search?feature=Studio%20Apartments&is-buy=false&show-let-agreed=false",
                    "http://acquireproperties.co.uk/property-search?feature=Studio&is-buy=false&show-let-agreed=false",
                    
                ],
                "property_type" : "studio"
            },      
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):

        script_data = response.xpath("//search-results/@actual-properties").get().replace("\n", "").strip()

        for item in json.loads(script_data):
            if "status" in item and item["status"] == 4:
                continue
            attribute_dict = {
                "property_type" : response.meta.get('property_type'),
                "address" : item["Address"] if "Address" in item else None,
                "price" : item["Price"] if "Price" in item else None,
                "room_count" : item["Bedrooms"] if "Bedrooms" in item else None,
                "bathroom_count" : item["Bathrooms"] if "Bathrooms" in item else None,
                "lat" : item["Latitude"] if "Latitude" in item else None,
                "lng" : item["Longitude"] if "Longitude" in item else None,
                "description" : remove_tags(item["Description"]) if "Description" in item else None,
                "images" : [response.urljoin(x) for x in item["Thumbnails"]] if "Thumbnails" in item else None,
            }
            follow_url = "http://acquireproperties.co.uk/property-details?id=" + item["PropertyID"]
            yield Request(follow_url, callback=self.populate_item, meta=attribute_dict)
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        
        item_loader.add_value("address", response.meta.get("address"))
        item_loader.add_value("rent", response.meta.get("price"))
        item_loader.add_value("currency", "GBP")
        item_loader.add_value("room_count", response.meta.get("room_count"))
        item_loader.add_value("bathroom_count", response.meta.get("bathroom_count"))
        
        item_loader.add_value("description", response.meta.get("description"))
        item_loader.add_value("images", response.meta.get("images"))
        item_loader.add_value("external_images_count", len(response.meta.get("images")))

        item_loader.add_value("external_source", "Acquireproperties_PySpider_" + self.country + "_" + self.locale)

        external_id = response.url.split('id=')[-1]
        if external_id:
            item_loader.add_value("external_id", external_id.strip())
        
        title = response.meta.get("address")
        if title:
            item_loader.add_value("title", title.strip())
        
        
        available_date = response.xpath("//property-details/@actual-property").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.split('"Available":')[1].split(',')[0].strip().strip('"'), date_formats=["%d/%m/%Y"])
            today = datetime.combine(date.today(), datetime.min.time())
            if date_parsed:
                result = today > date_parsed
                if result == True:
                    date_parsed = date_parsed.replace(year = today.year + 1)
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
            
        script_data = json.loads(response.xpath("//property-details/@actual-property").get().replace("\n", "").strip())
        if script_data:
            features = "".join(script_data["Bullets"])
            if features and ("garage" in features.lower() or "parking" in features.lower()):
                item_loader.add_value("parking", True)
            
            if "Longitude" in script_data and "Latitude" in script_data:
                item_loader.add_value("latitude", script_data["Latitude"])
                item_loader.add_value("longitude", script_data["Longitude"])
        
            if "Address" in script_data:
                city = script_data["Address"].split(",")[-1]
                item_loader.add_value("city", city)

        item_loader.add_value("landlord_name", "Burton Branch")
        item_loader.add_value("landlord_phone", "01283 564441")
        item_loader.add_value("landlord_email", "burton@acquireproperties.co.uk")
        
        yield item_loader.load_item()
