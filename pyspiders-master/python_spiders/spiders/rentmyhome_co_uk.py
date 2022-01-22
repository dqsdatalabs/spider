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


class MySpider(Spider):
    name = 'rentmyhome_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://secure.rentmyhome.co.uk/for-rent?utf8=%E2%9C%93&property_type=5&property_location=Show+all&price_min=0&price_max=10001&bedrooms=0",
                ],
                "property_type" : "apartment"
            },
            {
                "url" : [
                    "https://secure.rentmyhome.co.uk/for-rent?utf8=%E2%9C%93&property_type=2&property_location=Show+all&price_min=0&price_max=10001&bedrooms=0",
                    "https://secure.rentmyhome.co.uk/for-rent?utf8=%E2%9C%93&property_type=1&property_location=Show+all&price_min=0&price_max=10001&bedrooms=0",
                    "https://secure.rentmyhome.co.uk/for-rent?utf8=%E2%9C%93&property_type=3&property_location=Show+all&price_min=0&price_max=10001&bedrooms=0",
                    "https://secure.rentmyhome.co.uk/for-rent?utf8=%E2%9C%93&property_type=4&property_location=Show+all&price_min=0&price_max=10001&bedrooms=0"
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://secure.rentmyhome.co.uk/for-rent?utf8=%E2%9C%93&property_type=7&property_location=Show+all&price_min=0&price_max=10001&bedrooms=0",
                ],
                "property_type" : "room"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//a[contains(@class,'property')]"):
            status = item.xpath(".//span[contains(@class,'loz--primary')]/span/text()").get()
            if status and status.lower().strip() in ["let", "under offer"]:
                continue
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, dont_filter=True, callback=self.populate_item, meta={"url":follow_url, 'property_type': response.meta.get('property_type')})
        
        next_page = response.xpath("//a[@class='next_page']/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse, meta={'property_type': response.meta.get('property_type')})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        
        property_type = response.meta.get('property_type')
        ex_url = response.meta.get("url")
        external_link =  response.meta.get("url").split("-")[-1]
        if external_link:
            url = f"https://www.rentmyhome.co.uk/api/v1/get-property-by-id/property-{external_link}"
            yield Request(url, callback=self.parse_detail, meta={'property_type': property_type,"ex_url":ex_url})

    def parse_detail(self,response):
        
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type",response.meta.get('property_type') )
        item_loader.add_value("external_link",response.meta.get('ex_url'))
        item_loader.add_value("external_source","Rentmyhome_Co_PySpider_"+ self.country)
        
        j_seb = json.loads(response.body)
        data = j_seb["data"]["property"]

        title = ""
        if "address1" in data:
            title = title + data["address1"] + ", "
            if "town" in data:
                title = title + data["town"] + ", "
                if "postcode_start" in data:
                    title = title + data["postcode_start"]
    
        item_loader.add_value("title", title)

        if "outside_space" in data and data["outside_space"]:
            if "balcony" in data["outside_space"].lower():
                item_loader.add_value("balcony", True)
        
        if "property_type" in data and "terraced" in data["property_type"].lower():
            item_loader.add_value("terrace", True) 

        address1 = data["address1"]
        if address1:
            item_loader.add_value("address", address1)
        
        city = data["town"]
        if city:
            item_loader.add_value("city", city)
        
        zipcode = data["postcode"]
        if zipcode:
            item_loader.add_value("zipcode", zipcode)
        
            
        external_id = data["slug"]
        if external_id:
            item_loader.add_value("external_id", external_id.split("-")[1])
            
        available_date = data["available_at"]
        if available_date:
            date_parsed = dateparser.parse(
                        available_date, date_formats=["%d/%m/%Y"]
                    )
            date2 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date2)
        
        room_count = data["bedrooms"]
        room = data["receptions"]
        if room_count:
            item_loader.add_value("room_count", str(room_count))
        elif room:
            item_loader.add_value("room_count", str(room))
        
        bathroom_count = data["bathrooms"]
        if bathroom_count:
            item_loader.add_value("bathroom_count", str(bathroom_count))
        
        deposit = data["deposit"]
        if deposit:
            deposit = deposit.split("£")[1].replace(",","")
            item_loader.add_value("deposit", str(deposit))
        
        desc = data["description"]
        if desc:
            item_loader.add_value("description", desc)
        
        if desc and "per week" in desc.lower():
            rent = data["price"]
            if rent:
                price = rent.split("£")[1].replace(",","")
                item_loader.add_value("rent", int(price)*4)
        elif not "per week" in desc.lower():
            rent = data["price"]
            if rent:
                price = rent.split("£")[1].replace(",","")
                item_loader.add_value("rent", str(price))

        item_loader.add_value("currency", "GBP")
        externalcheck=item_loader.get_output_value("external_id")
        if externalcheck and externalcheck=="3104":
            return



        
        floor_plan_images = data["floorplans"]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images.get("thumb"))
        
        furnished = data["furnished"]
        if furnished:
            if "Unfurnished" in furnished:
                item_loader.add_value("furnished", False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished", True)

        latitude = data["latitude"]
        if latitude:
            item_loader.add_value("latitude", str(latitude))
            
        longitude = data["longitude"]
        if longitude:
            item_loader.add_value("longitude", str(longitude))
        
        parking = data["parking"]
        if parking:
            if "No parking" in parking:
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)
        
        images = data["photos"]
        for image in images:
            item_loader.add_value("images", image.get("thumb"))

        item_loader.add_value("landlord_name", "Rent My Home")
        item_loader.add_value("landlord_phone", "020 3875 6999")
        item_loader.add_value("landlord_email", "admin@rentmyhome.co.uk")
        
        yield item_loader.load_item()
