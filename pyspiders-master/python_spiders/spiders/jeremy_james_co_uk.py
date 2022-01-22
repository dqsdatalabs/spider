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
    name = 'jeremy_james_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://jeremy-james.co.uk/property_filter?transaction=lettings&tenure_type=0&prop_search_criteria=2&bedrooms=0&price_min=0&price_max=999999999999999&category=residential&orderBy=price&order=asc",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://jeremy-james.co.uk/property_filter?transaction=lettings&tenure_type=0&prop_search_criteria=1&bedrooms=0&price_min=0&price_max=999999999999999&category=residential&orderBy=price&order=asc"
                ],
                "property_type": "house"
            }
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    meta={'property_type': url.get('property_type')}
                )

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//script[contains(.,'properties.push')]//text()").extract():
            data = "["+item.split("push(")[1].split("});")[0]+"}]"
            data= json.loads(data)
            
            try:
                url = data[0]["portal_responses"]["rightmove"]["rightmove_preview_url"].split('href="')[-1].split('">')[0]
                print(url)
                # url="".join(url.split('"')[3:4])
            except:
                url = data[0]["portal_responses"]["rightmove"]["rightmove_preview_url"].split('href="')[-1].split('">',"")
                print(url)
                # url="".join(url.split('"')[3:4])
            yield Request(url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type'), "data":data})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        property_type = response.meta.get('property_type')
        prp_type_studio = response.xpath("//div[div[.='PROPERTY TYPE']]//div/div//text()[contains(.,'Studio')][normalize-space()]").get()
        if prp_type_studio:
            property_type = "studio"
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("external_source", "Jeremy_James_Co_PySpider_united_kingdom")

        data = response.meta.get('data')
        data = data[0]
        item_loader.add_value("external_id", str(data["id"]))
        item_loader.add_value("title", data["title"])
        item_loader.add_value("address", data["display_address"])
        item_loader.add_value("city", data["town"])
        item_loader.add_value("zipcode", f"{data['postcode_1']} {data['postcode_2']}")
        item_loader.add_value("rent", int(float(data['price'])))
        item_loader.add_value("currency", "GBP")
        
        if data["bedrooms"] !=0:
            item_loader.add_value("room_count", data["bedrooms"])
        else: item_loader.add_value("room_count", data["reception_rooms"])
        
        item_loader.add_value("bathroom_count", data["bathrooms"])
        item_loader.add_value("latitude", str(data["latitude"]))
        item_loader.add_value("longitude", str(data["longitude"]))
        
        description = data['description']
        sel = Selector(text=description, type="html")
        item_loader.add_value("description", "".join(sel.xpath(".//text()").getall()).strip())
        
        if data["garage"] !='0' or data["parking_spaces"] !='0' or data["outside_spaces"] !='0':
            item_loader.add_value("parking", True)
        
        item_loader.add_value("deposit", int(float(data['deposit'])))
        
        if data["furnished_state"] and "un" not in data["furnished_state"].lower():
            item_loader.add_value("furnished", True)
        
        for i in data["features"]:
            if "Furnished" in i or " furnished" in i:
                item_loader.add_value("furnished", True)
            if "lift" in i.lower():
                item_loader.add_value("elevator", True)
            if "epc" in i.lower():
                item_loader.add_value("energy_label", i.split(" ")[-1])
            if "terrace" in i.lower():
                item_loader.add_value("terrace", True)
            if "balcony" in i.lower():
                item_loader.add_value("balcony", True)
            if "garage" in i.lower() or "parking" in i.lower():
                item_loader.add_value("parking", True)
            if "sq" in i.lower():
                item_loader.add_value("square_meters", i.split("sq.m")[0].strip().split(" ")[-1])

        import dateparser
        available_date = data["date_available"]
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        if data["main_floorplan_image"]:
            floor_plan_images = [response.urljoin(x) for x in data["main_floorplan_image"]]
            item_loader.add_value("floor_plan_images", floor_plan_images)

        for x in data["slideshow_images"]:
            item_loader.add_value("images", f"https://jeremy-james.co.uk/uploads/{x}")
        
        item_loader.add_value("landlord_name", "Jeremy James and Company")
        item_loader.add_value("landlord_phone", "020 7486 4111")
        item_loader.add_value("landlord_email", "jjandco@jeremy-james.co.uk")
        
        yield item_loader.load_item()