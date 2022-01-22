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
    name = 'stonehouseproperties_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.stonehouseproperties.co.uk/property-search?is-professional=true",
                ],
                "property_type": "apartment"
            },
            {
                "url": [
                    "https://www.stonehouseproperties.co.uk/property-search?is-professional=false",
                ],
                "property_type": "student_apartment"
            },
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
        properties = response.xpath("//search-results/@actual-properties").get()
        prop = re.sub('\s{2,}', '', properties.strip().replace("\\n","").replace("\\","").replace("<br />",""))
        data = prop.split("{")
        for d in range(1,len(data)):
            item = data[d].split("}")[0]
            prop_id = item.split('"PropertyID": "')[1].split('"')[0]
            f_url = f"https://www.stonehouseproperties.co.uk/property-details?id={prop_id}"
            yield Request(f_url, callback=self.populate_item, meta={"property_type":response.meta.get('property_type'), "item":item})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("=")[1])
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Stonehouseproperties_Co_PySpider_united_kingdom")

        data = response.meta.get('item')
        address = data.split('Address": "')[1].split('"')[0]
        if address:
            item_loader.add_value("title", address)
            item_loader.add_value("address", address)
            if not address.split(",")[-1].strip().isalpha():
                zipcode = address.split(",")[-1].strip()
                if "." in zipcode:
                    item_loader.add_value("zipcode", zipcode.split(".")[1].strip())
                    item_loader.add_value("city", zipcode.split(".")[0].strip())
                else:
                    item_loader.add_value("zipcode", zipcode)
                    item_loader.add_value("city",address.split(",")[-2].strip())
            else: item_loader.add_value("city", address.split(",")[-1].strip())
        
        rent = data.split('Price": "')[1].split('"')[0]
        if rent:
            rent = rent.split(".00")[0]
            if response.meta.get('property_type') == "student_apartment": rent = int(float(rent))*4
            item_loader.add_value("rent", int(float(rent)))
        item_loader.add_value("currency", "GBP")
        
        room_count = data.split('Bedrooms":"')[1].split('"')[0]
        if room_count and room_count !='0':
            item_loader.add_value("room_count", room_count)
        
        bathroom_count = data.split('Bathrooms":"')[1].split('"')[0]
        if bathroom_count and bathroom_count !='0':
            item_loader.add_value("bathroom_count", bathroom_count)
        
        item_loader.add_value("longitude", data.split('Longitude":"')[1].split('"')[0])
        item_loader.add_value("latitude", data.split('Latitude":"')[1].split('"')[0])
        
        description = data.split('Description":"')[1].split('"')[0]
        item_loader.add_value("description", description)
        
        available_date = data.split('Available": "')[1].split('"')[0]
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        images = data.split('Thumbnails":')[1].strip()
        if images:
            img = json.loads(images)
            for i in img:
                item_loader.add_value("images", response.urljoin(i))
        
        item_loader.add_value("landlord_name", "Stonehouse Properties")
        item_loader.add_value("landlord_phone", "0113 275 0777")
        item_loader.add_value("landlord_email", "info@stonehouseproperties.co.uk")
        
        status = data.split('Status":')[1].split(',')[0]
        if status == "0":
            yield item_loader.load_item()